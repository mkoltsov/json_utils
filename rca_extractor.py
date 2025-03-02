import os
import json
import re
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from atlassian import Confluence

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='rca_extraction.log'
)
logger = logging.getLogger('rca_extractor')

# Function to check if a page is filled with data or just a template
def is_template_page(soup):
    """
    Check if the page is just a template without real data
    Returns True if the page appears to be just a template
    """
    # Check if sections are empty or contain placeholder text
    sections = [
        "Incident General Information",
        "Summary",
        "Root Cause",
        "Actions Taken",
        "Timeline of Events",
        "Lessons Learned",
        "Planned Actions"
    ]

    # If most sections are empty, it's likely a template
    empty_sections = 0

    for section in sections:
        section_header = soup.find(string=re.compile(f"^{section}$", re.IGNORECASE))
        if section_header:
            # Get the content after this header
            next_elements = []
            current = section_header.parent.find_next_sibling()

            # Find the next section or end of content
            while current and not current.find(string=lambda text: any(s in text for s in sections) if text else False):
                next_elements.append(current)
                current = current.find_next_sibling()

            # Check if content seems empty or has just placeholder text
            content = ' '.join([e.get_text(strip=True) for e in next_elements if e])
            if not content or "TLDR:" in content or "Example:" in content or len(content) < 50:
                empty_sections += 1

    # If more than half of sections are empty, consider it a template
    return empty_sections > len(sections) // 2

# Function to extract data from a specific section
def extract_section_data(soup, section_name):
    """
    Extract text data from a specific section in the page
    """
    section_header = soup.find(string=re.compile(f"^{section_name}$", re.IGNORECASE))
    if not section_header:
        return ""

    # Get the content after this header until the next section
    section_content = []
    current = section_header.parent.find_next_sibling()

    # Find all sections to know when to stop
    all_sections = [
        "Incident General Information",
        "Summary",
        "Root Cause",
        "Actions Taken",
        "Timeline of Events",
        "Lessons Learned",
        "Planned Actions"
    ]

    # Collect content until next section or end of content
    while current and not current.find(string=lambda text: any(s in text for s in all_sections) if text else False):
        # Extract text and clean it
        text = current.get_text(strip=True, separator=' ')
        if text:
            section_content.append(text)
        current = current.find_next_sibling()

    return ' '.join(section_content)

# Function to extract incident number from page title or content
def extract_incident_number(soup, page_title):
    """
    Extract the incident number from the page title or content
    """
    # First try to get from title using regex (INC-XXXX pattern)
    match = re.search(r'(INC-\d+)', page_title)
    if match:
        return match.group(1)

    # Try to find it in the General Information section
    info_section = soup.find(string=re.compile("Incident General Information", re.IGNORECASE))
    if info_section:
        # Look for a table with incident number
        table = info_section.find_next('table')
        if table:
            incident_cell = table.find(string=re.compile("Incident #", re.IGNORECASE))
            if incident_cell:
                # Get the next cell which should contain the incident number
                row = incident_cell.find_parent('tr')
                if row:
                    cells = row.find_all('td')
                    if len(cells) > 1:
                        return cells[1].get_text(strip=True)

    # If no incident number found, use timestamp
    return f"UNKNOWN-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# Function to extract data from RCA page and save to JSON
def process_rca_page(confluence, page_id, processed_ids):
    """
    Process a single RCA page, extract data and save to JSON
    """
    try:
        # Skip if already processed
        if page_id in processed_ids:
            logger.info(f"Skipping already processed page {page_id}")
            return True

        # Get page content
        page = confluence.get_page_by_id(page_id, expand='body.storage')
        page_title = page['title']
        html_content = page['body']['storage']['value']

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Check if this is an empty template
        if is_template_page(soup):
            logger.info(f"Skipping template page: {page_title}")
            return False

        # Extract incident number
        incident_number = extract_incident_number(soup, page_title)

        # Extract data from each section
        rca_data = {
            "title": page_title,
            "page_id": page_id,
            "extraction_date": datetime.now().isoformat(),
            "sections": {
                "Incident General Information": extract_section_data(soup, "Incident General Information"),
                "Summary": extract_section_data(soup, "Summary"),
                "Root Cause": extract_section_data(soup, "Root Cause"),
                "Actions Taken": extract_section_data(soup, "Actions Taken"),
                "Timeline of Events": extract_section_data(soup, "Timeline of Events"),
                "Lessons Learned": extract_section_data(soup, "Lessons Learned"),
                "Planned Actions": extract_section_data(soup, "Planned Actions")
            }
        }

        # Save to JSON file
        filename = f"{incident_number}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(rca_data, f, ensure_ascii=False, indent=4)

        logger.info(f"Saved RCA data to {filename}")
        return True

    except Exception as e:
        logger.error(f"Error processing page {page_id}: {str(e)}")
        return False

# Main function
def main():
    # Get credentials from environment variables
    username = os.environ.get('OKTA_USER')
    password = os.environ.get('OKTA_PASSWORD')

    if not username or not password:
        logger.error("Missing environment variables OKTA_USER or OKTA_PASSWORD")
        return

    # Confluence URL - replace with your actual Confluence URL
    confluence_url = "https://your-instance.atlassian.net"

    # Space key - replace with your actual space key
    space_key = "RCA"  # or whatever your RCA space key is

    # Connect to Confluence
    try:
        confluence = Confluence(
            url=confluence_url,
            username=username,
            password=password
        )

        logger.info("Successfully connected to Confluence")

        # Load processed IDs from file if it exists
        processed_ids = []
        if os.path.exists('processed.json'):
            with open('processed.json', 'r') as f:
                processed_ids = json.load(f)

        # Get all pages in the space
        all_pages = []
        start = 0
        limit = 100

        while True:
            pages = confluence.get_all_pages_from_space(space_key, start=start, limit=limit)
            if not pages:
                break
            all_pages.extend(pages)
            start += limit
            if len(pages) < limit:
                break

        logger.info(f"Found {len(all_pages)} pages in space {space_key}")

        # Process each page
        newly_processed = []

        for page in all_pages:
            page_id = page['id']

            # Process the page
            if process_rca_page(confluence, page_id, processed_ids):
                newly_processed.append(page_id)

        # Update processed IDs file
        processed_ids.extend(newly_processed)
        with open('processed.json', 'w') as f:
            json.dump(processed_ids, f)

        logger.info(f"Processed {len(newly_processed)} new pages")

    except Exception as e:
        logger.error(f"Error connecting to Confluence: {str(e)}")

if __name__ == "__main__":
    main()
