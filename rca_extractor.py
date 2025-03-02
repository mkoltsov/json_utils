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
    Check if the page is just a template without real data.
    Returns True if all sections are either empty or contain template indicators
    """
    key_sections = [
        "Summary",
        "Root Cause",
        "Actions Taken",

        "Lessons Learned",
        "Planned Actions"
    ]

    for section in key_sections:
        content = extract_section_data(soup, section)
        # Return False if section has content and doesn't contain template indicators
        if content and "Example:" in content:
            return True

    return False

# Function to extract data from a specific section
def extract_section_data(soup, section_name):
    """
    Extract text data from a specific section in the page by finding text between headers
    """
    logger.info(f"Attempting to extract section: {section_name}")

    # First, get all text content with minimal processing
    text_content = soup.get_text(separator='\n', strip=True)

    # Define all possible section headers
    sections = [
        "Incident General Information",
        "Summary",
        "Root Cause",
        "Actions Taken",
        "Timeline of Events",
        "Lessons Learned",
        "Planned Actions",
        "Details"
    ]

    try:
        # Find the start of our target section
        section_start = text_content.index(section_name)

        # Find the start of the next section
        next_section_pos = float('inf')
        for section in sections:
            if section == section_name:
                continue
            try:
                pos = text_content.index(section, section_start + len(section_name))
                next_section_pos = min(next_section_pos, pos)
            except ValueError:
                continue

        # Extract text between current section and next section
        if next_section_pos != float('inf'):
            section_text = text_content[section_start + len(section_name):next_section_pos]
        else:
            section_text = text_content[section_start + len(section_name):]

        # Clean up the extracted text
        section_text = section_text.strip()

        logger.info(f"Extracted {section_name} ({len(section_text)} chars): {section_text[:200]}...")
        return section_text

    except ValueError:
        logger.warning(f"Section not found: {section_name}")
        return ""
# Function to extract incident number from page title or content
def extract_incident_number(soup, page_title):
    """
    Extract the incident number from the page title or content
    """
    # First try to get from title using regex (INC-XXXX pattern)
    match = re.search(r'((?:INC|PRE)-\d+)', page_title)
    if match:
        return match.group(1)

    # Try to find it in the General Information section
    info_section = soup.find(string=re.compile("Incident General Information", re.IGNORECASE))
    if info_section:
        # Look for a table with incident number
        table = info_section.find_parent().find_next('table')
        if table:
            incident_row = table.find(string=re.compile("Incident #", re.IGNORECASE))
            if incident_row:
                # Get the next cell which should contain the incident number
                row = incident_row.find_parent('tr')
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
        if page_id in processed_ids:
            logger.info(f"Skipping already processed page {page_id}")
            return None

        page = confluence.get_page_by_id(page_id, expand='body.storage')
        page_title = page.get('title', 'Unknown Title')
        html_content = page['body']['storage']['value']

        soup = BeautifulSoup(html_content, 'html.parser')

        if is_template_page(soup):
            logger.info(f"Skipping template page: {page_title}")
            return None

        # Extract all sections
        rca_data = {
            "title": page_title,
            "page_id": page_id,
            "extraction_date": datetime.now().isoformat(),
            "sections": {}
        }

        sections = {
            "Incident General Information": extract_section_data(soup, "Incident General Information"),
            "Summary": extract_section_data(soup, "Summary"),
            "Root Cause": extract_section_data(soup, "Root Cause"),
            "Actions Taken": extract_section_data(soup, "Actions Taken"),
            "Timeline of Events": extract_section_data(soup, "Timeline of Events"),
            "Lessons Learned": extract_section_data(soup, "Lessons Learned"),
            "Planned Actions": extract_section_data(soup, "Planned Actions")
        }

        # Verify that at least Summary and Root Cause are present
        if not sections["Summary"] or not sections["Root Cause"]:
            logger.warning(f"Skipping page {page_id} - Missing Summary or Root Cause content")
            return None

        rca_data["sections"] = sections

        # Log all section contents for verification
        logger.info(f"Extracted content for page {page_id}:")
        for section, content in sections.items():
            logger.info(f"{section}: {len(content)} chars")
            logger.info(f"Content preview: {content[:200]}...")

        # Save to JSON file
        incident_number = extract_incident_number(soup, page_title)
        filename = f"{incident_number}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(rca_data, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully saved RCA data to {filename}")
        return page_id

    except Exception as e:
        logger.error(f"Error processing page {page_id}: {str(e)}")
        return None

# Function to process child pages
def process_child_pages(confluence, parent_id, processed_ids):
    """
    Process all child pages of a given parent page ID
    """
    try:
        # Get all child pages using get_child_pages or get_child_id_list method
        # The method varies based on the version of the Atlassian Python API
        try:
            # Try the get_child_id_list method first
            child_ids = confluence.get_child_id_list(parent_id)
            if not isinstance(child_ids, list):
                raise AttributeError("get_child_id_list method returned non-list result")

            logger.info(f"Found {len(child_ids)} child pages for parent {parent_id} using get_child_id_list")

            # Process each child page by ID
            newly_processed = []
            for index, child_id in enumerate(child_ids):
                logger.info(f"Processing child page {index+1}/{len(child_ids)}: ID {child_id}")

                # Process the page
                if process_rca_page(confluence, child_id, processed_ids):
                    newly_processed.append(child_id)

                # Recursively process children of this page
                child_processed = process_child_pages(confluence, child_id, processed_ids)
                newly_processed.extend(child_processed)

            return newly_processed

        except (AttributeError, TypeError) as e:
            # Fall back to the content API endpoint directly
            logger.info(f"get_child_id_list method not available, using direct API call: {str(e)}")

            # Use the REST API directly to get child pages
            url = f"{confluence.url}/rest/api/content/{parent_id}/child/page?expand=page"
            response = confluence.get(url)

            if not isinstance(response, dict) or 'results' not in response:
                logger.error(f"Unexpected response format when retrieving child pages of {parent_id}")
                return []

            child_pages = response.get('results', [])
            logger.info(f"Found {len(child_pages)} child pages for parent {parent_id} using direct API")

            newly_processed = []

            # Process each child page
            for index, page in enumerate(child_pages):
                # Ensure page has an id
                if not isinstance(page, dict) or 'id' not in page:
                    logger.error(f"Invalid page structure at index {index}: {type(page)}")
                    continue

                page_id = page['id']
                page_title = page.get('title', 'Unknown')
                logger.info(f"Processing child page {index+1}/{len(child_pages)}: {page_title} (ID: {page_id})")

                # Process the page
                if process_rca_page(confluence, page_id, processed_ids):
                    newly_processed.append(page_id)

                # Recursively process children of this page
                child_processed = process_child_pages(confluence, page_id, processed_ids)
                newly_processed.extend(child_processed)

            return newly_processed

    except Exception as e:
        logger.error(f"Error processing child pages of {parent_id}: {str(e)}")
        return []

# Load configuration from config.json
def load_config():
    """
    Load configuration from config.json file
    """
    try:
        if not os.path.exists('config.json'):
            logger.error("config.json file not found")
            raise FileNotFoundError("config.json file not found")

        with open('config.json', 'r') as f:
            config = json.load(f)

        # Validate required configuration
        required_keys = ['confluence_url', 'page_id']
        for key in required_keys:
            if key not in config:
                logger.error(f"Missing required configuration key: {key}")
                raise KeyError(f"Missing required configuration key: {key}")

        return config
    except json.JSONDecodeError:
        logger.error("Invalid JSON in config.json")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise

# Main function
def main():
    try:
        # Load configuration
        config = load_config()
        confluence_url = config['confluence_url']
        page_id = config['page_id']

        # Get credentials from environment variables
        username = os.environ.get('OKTA_USER')
        password = os.environ.get('OKTA_PASSWORD')

        if not username or not password:
            logger.error("Missing environment variables OKTA_USER or OKTA_PASSWORD")
            return

        # Connect to Confluence
        try:
            confluence = Confluence(
                url=confluence_url,
                username=username,
                password=password
            )

            logger.info(f"Successfully connected to Confluence at {confluence_url}")
            logger.info(f"Using page ID: {page_id}")

            # Test connection and verify page access
            try:
                # Simple API call to verify connection and page access
                page_info = confluence.get_page_by_id(page_id, expand='')
                if not page_info:
                    logger.error(f"Could not retrieve information for page {page_id}")
                    return
                logger.info(f"Successfully verified access to page: {page_info.get('title', page_id)}")
            except Exception as e:
                logger.error(f"Failed to verify page access: {str(e)}")
                return

            # Load processed IDs from file if it exists
            processed_ids = []
            if os.path.exists('processed.json'):
                with open('processed.json', 'r') as f:
                    processed_ids = json.load(f)

            # Process the specified page first
            newly_processed = []
            if process_rca_page(confluence, page_id, processed_ids):
                newly_processed.append(page_id)

            # Then process all child pages recursively
            child_processed = process_child_pages(confluence, page_id, processed_ids)
            newly_processed.extend(child_processed)

            # Update processed IDs file
            processed_ids.extend(newly_processed)
            with open('processed.json', 'w') as f:
                json.dump(processed_ids, f)

            logger.info(f"Processed {len(newly_processed)} new pages")

        except Exception as e:
            logger.error(f"Error connecting to Confluence: {str(e)}")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
