#!/usr/bin/env python3

import json
import os
import sys
from datetime import datetime, timezone, timedelta
import re
from typing import Dict, List, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from collections import Counter
import string

def print_header():
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {current_time}")
    print(f"Current User's Login: {os.getenv('USER', 'unknown')}")
    print("----------------------------------------\n")

def clean_text(text: str) -> str:
    """Remove formatting markers and normalize text"""
    # Remove specific markers
    text = re.sub(r'#[a-fA-F0-9]{6}\s*\nINLINE', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text

def calculate_text_quality_score(text: str) -> float:
    """Calculate quality score for text"""
    if not text:
        return 0.0

    text = clean_text(text)
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    if not sentences:
        return 0.0

    # Calculate sentence length variation
    sentence_lengths = [len(s.split()) for s in sentences]
    sentence_length_var = np.std(sentence_lengths) if len(sentence_lengths) > 1 else 0

    # Calculate word length variation
    words = text.split()
    word_lengths = [len(w) for w in words]
    word_length_var = np.std(word_lengths) if len(word_lengths) > 1 else 0

    # Information density
    unique_words = len(set(words))
    info_density = unique_words / len(words) if words else 0

    # Technical terms presence
    technical_terms = {'aws', 'api', 'service', 'system', 'error', 'failure', 'impact', 'resolution',
                      'metrics', 'data', 'configuration', 'outage', 'incident', 'region', 'server',
                      'database', 'network', 'infrastructure', 'deployment', 'monitoring'}
    tech_term_count = sum(1 for word in words if word.lower() in technical_terms)
    tech_term_ratio = tech_term_count / len(words) if words else 0

    score = (
        0.3 * min(1.0, sentence_length_var / 10) +
        0.2 * min(1.0, word_length_var / 3) +
        0.3 * info_density +
        0.2 * min(1.0, tech_term_ratio * 5)
    )

    return score

def search_text_in_section(section_text: str, search_text: str) -> Tuple[bool, str]:
    """Search for text in a section and return context if found"""
    if not section_text or not search_text:
        return False, ""

    clean_section = clean_text(section_text).lower()
    search_terms = clean_text(search_text).lower()

    # Try exact match first
    if search_terms in clean_section:
        # Get context around the match
        words = clean_section.split()
        search_words = search_terms.split()

        for i in range(len(words) - len(search_words) + 1):
            if ' '.join(words[i:i+len(search_words)]) == search_terms:
                start = max(0, i - 10)
                end = min(len(words), i + len(search_words) + 10)
                context = ' '.join(words[start:end])
                return True, f"...{context}..."

    return False, ""

def extract_dates(text: str) -> List[datetime]:
    """Extract dates from text in various formats"""
    dates = []
    patterns = [
        r'\b(\d{4}-\d{2}-\d{2})\b',
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* (\d{1,2}),? (\d{4})\b',
        r'\b(\d{1,2}) (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* (\d{4})\b',
        r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]* (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* (\d{1,2}),? (\d{4})\b'
    ]

    months_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    for line in text.split('\n'):
        for pattern in patterns:
            matches = re.finditer(pattern, line, re.IGNORECASE)
            for match in matches:
                try:
                    if len(match.groups()) == 1:
                        dates.append(datetime.strptime(match.group(1), "%Y-%m-%d"))
                    else:
                        month_match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b',
                                             line,
                                             re.IGNORECASE)
                        if month_match:
                            month = months_map[month_match.group(1).lower()[:3]]
                            if len(match.groups()) == 2:
                                day = int(match.group(1))
                                year = int(match.group(2))
                                dates.append(datetime(year, month, day))
                except ValueError:
                    continue
    return dates

def get_newest_date(dates: List[datetime]) -> Tuple[datetime, str]:
    if not dates:
        return None, ""
    newest_date = max(dates)
    return newest_date, newest_date.strftime("%Y-%m-%d")

def process_json_files(search_text: str) -> List[Tuple[str, str, Dict[str, str], float, int]]:
    """Process JSON files and return matches with section matches"""
    matches = []

    json_files = [f for f in os.listdir('.') if f.endswith('.json')]

    for filename in json_files:
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except (json.JSONDecodeError, FileNotFoundError, PermissionError) as e:
            print(f"Warning: Cannot process file {filename} - {str(e)}")
            continue

        if not isinstance(data, dict) or 'sections' not in data:
            continue

        sections = data.get('sections', {})

        # Search in all sections
        section_matches = {}
        found = False
        for section_name, section_text in sections.items():
            match_found, context = search_text_in_section(section_text, search_text)
            if match_found:
                section_matches[section_name] = context
                found = True

        if not found:
            continue

        # Get dates
        incident_dates = extract_dates(sections.get("Incident General Information", ""))
        summary_dates = extract_dates(sections.get("Summary", ""))
        newest_date, date_str = get_newest_date(incident_dates + summary_dates)

        if not newest_date or not is_within_two_years(newest_date):
            continue

        # Calculate quality score based on matching sections
        quality_score = max(calculate_text_quality_score(text) for text in section_matches.values())

        # Calculate total word count of matching sections
        word_count = sum(len(clean_text(text).split()) for text in section_matches.values())

        matches.append((filename, date_str, section_matches, quality_score, word_count))

    return matches

def select_best_documents(matches: List[Tuple[str, str, Dict[str, str], float, int]], max_words: int = 24000) -> List[Tuple[str, str, Dict[str, str]]]:
    """Select best documents while keeping total word count under limit"""
    sorted_matches = sorted(matches, key=lambda x: x[3], reverse=True)

    selected = []
    total_words = 0

    for match in sorted_matches:
        if total_words + match[4] <= max_words:
            selected.append((match[0], match[1], match[2]))
            total_words += match[4]

    return selected

def main():
    print_header()

    if len(sys.argv) != 2:
        print("Usage: search_root_cause.py \"search text\"")
        sys.exit(1)

    search_text = sys.argv[1]
    all_matches = process_json_files(search_text)

    if not all_matches:
        print(f"No matches found for \"{search_text}\" in any section")
        return

    selected_matches = select_best_documents(all_matches)

    total_words = sum(match[4] for match in all_matches if match[:3] in selected_matches)

    print(f"Selected {len(selected_matches)} documents (Total words: {total_words})")
    print("----------------------------------------\n")

    for filename, date_str, section_matches in selected_matches:
        print(f"\nFound in file: {filename}")
        print(f"Date: {date_str}")
        print("Matches found in sections:")
        print("----------------------------------------")
        for section_name, context in section_matches.items():
            print(f"\n{section_name}:")
            print(context)
        print("----------------------------------------")

    print("\nSummary of selected documents:")
    print("----------------------------------------")
    for filename, date_str, _ in selected_matches:
        print(f"Found in file: {filename}")
        print(f"  Date: {date_str}")

def is_within_two_years(date: datetime) -> bool:
    two_years_ago = datetime.now() - timedelta(days=730)
    return date >= two_years_ago

if __name__ == "__main__":
    main()
