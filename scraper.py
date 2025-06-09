
import pandas as pd
import requests
import time
import os
import json
from dotenv import load_dotenv
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import signal
import sys
from fuzzywuzzy import fuzz

load_dotenv()
SLEEP_DELAY = 1.5
CHECKPOINT_FILE = "search_checkpoint.json"
RESULTS_FILE = "search_results_backup.json"
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 10

class CheckpointManager:
    @staticmethod
    def save(current_index=None, results_data=None):
        checkpoint_data = {
            'last_processed_index': current_index,
            'timestamp': time.time(),
            'results_count': len(results_data) if results_data else 0
        }
        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(checkpoint_data, f)
            if results_data:
                with open(RESULTS_FILE, 'w') as f:
                    json.dump(results_data, f, default=str)
        except Exception as e:
            print(f"❌ Error saving checkpoint: {e}")

    @staticmethod
    def load():
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, 'r') as f:
                    checkpoint = json.load(f)
                with open(RESULTS_FILE, 'r') as f:
                    results_data = json.load(f)
                return checkpoint.get('last_processed_index', -1), results_data
        except Exception as e:
            print(f"❌ Error loading checkpoint: {e}")
        return -1, []

class DataProcessor:
    @staticmethod
    def clean_data(df):
        df = df.loc[:, ~df.columns.duplicated()]
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.dropna(subset=['name', 'school', 'graduation_year'])
        df = df.drop_duplicates(subset=['name', 'school', 'graduation_year'])
        df['graduation_year'] = pd.to_numeric(df['graduation_year'], errors='coerce')
        df = df.dropna(subset=['graduation_year'])
        df['graduation_year'] = df['graduation_year'].astype(int)
        df['clean_name'] = df['name'].str.strip()
        df['clean_school'] = df['school'].str.strip()
        return df.reset_index(drop=True)

class LinkedInSearcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = self._create_session()
        self.search_url = "https://serpapi.com/search"

    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def create_queries(self, row):
        name = row['clean_name']
        school = row['clean_school']
        grad_year = str(int(row['graduation_year']))
        return [
            f'"{name}" "{school}" {grad_year} site:linkedin.com/in',
            f'{name} {school} {grad_year} site:linkedin.com/in',
            f'"{name}" "{school}" site:linkedin.com/in'
        ]

    def search_person(self, queries, original_name):
        best_result = None
        best_score = 0
        for i, query in enumerate(queries):
            print(f"  Query {i+1}/{len(queries)}: {query}")
            params = {"q": query, "engine": "google", "api_key": self.api_key, "num": 5}
            try:
                response = self.session.get(self.search_url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    for result in data.get('organic_results', []):
                        if 'linkedin.com/in' in result.get('link', ''):
                            score = self._score_result(original_name, result)
                            if score > best_score:
                                result['search_score'] = score
                                result['query_used'] = query
                                best_result = result
                                best_score = score
                                if score >= 90:
                                    return best_result
                time.sleep(SLEEP_DELAY)
            except Exception as e:
                print(f"    Error: {e}")
                time.sleep(2)
        return best_result

    def _score_result(self, search_name, result):
        title = result.get('title', '')
        snippet = result.get('snippet', '')
        extracted_name = self._extract_name_from_title(title)
        if search_name.lower() != extracted_name.lower():
            return 0
        score = fuzz.token_sort_ratio(search_name.lower(), extracted_name.lower())
        if any(term in title.lower() or term in snippet.lower() for term in ['ceo', 'founder', 'partner']):
            score += 10
        return min(score, 100)

    def _extract_name_from_title(self, title):
        if not title:
            return ""
        match = re.search(r"^([^-|]+)", title)
        return match.group(1).strip() if match else title.split()[0]

class ProminenceFilter:
    PROMINENCE_KEYWORDS = {
        'ceo': 15, 'founder': 12, 'partner': 10,
        'president': 10, 'manager': 5, 'professional athlete': 10
    }
    @staticmethod
    def calculate_score(row):
        combined = f"{row.get('title', '').lower()} {row.get('snippet', '').lower()}"
        score = row.get('search_score', 0)
        for keyword, pts in ProminenceFilter.PROMINENCE_KEYWORDS.items():
            if keyword in combined:
                score += pts
        return score

    
    @staticmethod
    def is_valid_match(row):
        search_name = row['clean_name'].lower()
        title = str(row.get('title', '')).lower()
        snippet = str(row.get('snippet', '')).lower()
        grad_year = str(row.get('graduation_year', ''))

        # Must contain the name AND "harvard"
        if search_name not in title:
            return False
        if "harvard" not in title and "harvard" not in snippet:
            return False

        # Try to verify the year is present and reasonable
        if grad_year and grad_year in snippet:
            return True
        elif grad_year:
            # Look for snippets that contradict expected grad year
            for y in range(int(grad_year) + 3, int(grad_year) + 12):
                if str(y) in snippet:
                    return False
        return True

def main():
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SERPAPI_API_KEY missing")

    df = pd.read_csv("Baseball_roster.csv")
    df = DataProcessor.clean_data(df)
    df = df[df['clean_school'].str.contains("Harvard", case=False, na=False)]

    searcher = LinkedInSearcher(api_key)
    start_index, results_data = CheckpointManager.load()
    start_index += 1

    for idx in range(start_index, len(df)):
        row = df.iloc[idx]
        print(f"\n🔍 Searching {idx+1}/{len(df)}: {row['clean_name']}")
        queries = searcher.create_queries(row)
        result = searcher.search_person(queries, row['clean_name'])

        record = {
            'name': row['name'],
            'clean_name': row['clean_name'],
            'school': row['school'],
            'graduation_year': row['graduation_year'],
            'sport': row.get('sport', ''),
            'title': result.get('title') if result else None,
            'linkedin_url': result.get('link') if result else None,
            'snippet': result.get('snippet') if result else None,
            'search_score': result.get('search_score', 0) if result else 0,
            'query_used': result.get('query_used') if result else None
        }

        results_data.append(record)
        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            CheckpointManager.save(idx, results_data)

    results_df = pd.DataFrame(results_data)
    results_df['is_valid_match'] = results_df.apply(ProminenceFilter.is_valid_match, axis=1)
    valid_df = results_df[results_df['is_valid_match']].copy()
    valid_df['prominence_score'] = valid_df.apply(ProminenceFilter.calculate_score, axis=1)

    timestamp = int(time.time())
    # Removed Tier 1 output(f"tier1_top_prominent_{timestamp}.csv", index=False)
    # Removed Tier 2 output(f"tier2_likely_prominent_{timestamp}.csv", index=False)
    # Removed Tier 3 output(f"tier3_review_{timestamp}.csv", index=False)

    valid_df = valid_df[valid_df['prominence_score'] >= 5]
    valid_df.to_csv(f"valid_high_prominence_{timestamp}.csv", index=False)
    
    
    

if __name__ == "__main__":
    main()
