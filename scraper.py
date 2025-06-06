import pandas as pd
import requests
import time

# === STEP 1: LOAD YOUR CSV ===
df = pd.read_csv("Baseball_roster.csv")

# Optional: Show columns
print("Columns before cleanup:", df.columns.tolist())
print("Any duplicate columns?:", df.columns.duplicated().any())
print("Shape before cleanup:", df.shape)

# === STEP 2: CLEANUP ===
# Drop duplicate columns first
df = df.loc[:, ~df.columns.duplicated()]

# Clean column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Rename for consistency - only rename team since graduation_year already exists
df.rename(columns={
    'team': 'sport'
}, inplace=True)

# If you want to use last_seen_year instead of graduation_year, uncomment the line below:
# df.rename(columns={'last_seen_year': 'graduation_year'}, inplace=True)
# df.drop(columns=['graduation_year'], inplace=True)  # Remove the original graduation_year column

# === STEP 3: HANDLE DUPLICATES AND MISSING DATA ===
print("Shape after column cleanup:", df.shape)

# Drop rows with missing critical data
df = df.dropna(subset=['name', 'school', 'graduation_year'])
print("Shape after dropping rows with missing critical data:", df.shape)

# Remove duplicate rows based on key columns
df = df.drop_duplicates(subset=['name', 'school', 'graduation_year'], keep='first')
print("Shape after removing duplicate rows:", df.shape)

# Reset index to fix any index issues
df.reset_index(drop=True, inplace=True)

# === STEP 4: VERIFY REQUIRED COLUMNS EXIST ===
required = ['name', 'school', 'graduation_year']
for col in required:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# === STEP 5: CREATE LINKEDIN SEARCH QUERIES ===
# Debug: Check what we have in graduation_year column
print("Graduation year column info:")
print(f"Type: {type(df['graduation_year'])}")
print(f"Sample values: {df['graduation_year'].head()}")
print(f"Data types: {df['graduation_year'].dtype}")

# Handle potential data type issues
try:
    df['graduation_year'] = pd.to_numeric(df['graduation_year'], errors='coerce')
    df = df.dropna(subset=['graduation_year'])  # Remove rows where graduation_year couldn't be converted
    df['graduation_year'] = df['graduation_year'].astype(int)
    print(f"Successfully converted graduation_year to numeric. Shape: {df.shape}")
except Exception as e:
    print(f"Error converting graduation_year: {e}")
    print("Attempting alternative approach...")
    
    # Alternative: ensure it's a proper Series
    if 'graduation_year' in df.columns:
        df = df.reset_index(drop=True)
        grad_year_series = df['graduation_year'].copy()
        df['graduation_year'] = pd.to_numeric(grad_year_series, errors='coerce')
        df = df.dropna(subset=['graduation_year'])
        df['graduation_year'] = df['graduation_year'].astype(int)
    else:
        raise ValueError("graduation_year column not found after renaming")

# Create search queries with better error handling
def create_search_query(row):
    try:
        name = str(row['name']).strip()
        school = str(row['school']).strip()
        grad_year = str(int(row['graduation_year']))
        return f"{name} {school} {grad_year} site:linkedin.com/in"
    except Exception as e:
        print(f"Error creating query for row: {row.name}, Error: {e}")
        return None

df['search_query'] = df.apply(create_search_query, axis=1)

# Remove rows where search_query creation failed
df = df.dropna(subset=['search_query'])
df.reset_index(drop=True, inplace=True)

print(f"Final dataset shape: {df.shape}")
print("Sample search queries:")
print(df['search_query'].head(3).tolist())

# === STEP 6: SERPAPI CONFIG ===
SERPAPI_API_KEY = "157921b655a573846615c76e52a574b2bb1a00816b97bdcb7f5ef6cc517df709"
search_url = "https://serpapi.com/search"

def search_linkedin(query):
    params = {
        "q": query,
        "engine": "google",
        "api_key": SERPAPI_API_KEY
    }
    try:
        response = requests.get(search_url, params=params)
        if response.status_code != 200:
            print(f"Error {response.status_code} for query: {query}")
            return None
        return response.json()
    except Exception as e:
        print(f"Request failed for query '{query}': {e}")
        return None

# === STEP 7: SCRAPE RESULTS ===
titles, urls, snippets = [], [], []

for idx, row in df.iterrows():
    query = row['search_query']
    print(f"Searching {idx + 1}/{len(df)}: {query}")
    
    result = search_linkedin(query)
    time.sleep(1.5)  # Pause to respect rate limits

    found = False
    if result and 'organic_results' in result:
        for r in result['organic_results']:
            if 'linkedin.com/in' in r.get('link', ''):
                titles.append(r.get('title', ''))
                urls.append(r.get('link', ''))
                snippets.append(r.get('snippet', ''))
                found = True
                break
    
    if not found:
        titles.append(None)
        urls.append(None)
        snippets.append(None)

# Add results to dataframe
df['title'] = titles
df['linkedin_url'] = urls
df['snippet'] = snippets

# === STEP 8: FILTER FOR TARGET ROLES ===
keywords = ['ceo', 'founder', 'president', 'vp', 'cofounder', 'partner', 'dr', 'md', 'athlete', 'professional']

def check_title_match(title):
    if not isinstance(title, str):
        return False
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in keywords)

df['matched_title'] = df['title'].apply(check_title_match)

filtered_df = df[df['matched_title']].copy()

# === STEP 9: EXPORT RESULTS ===
df.to_csv("all_results.csv", index=False)
filtered_df.to_csv("filtered_professionals.csv", index=False)

print("\n✅ Done! Results saved as:")
print(f" - all_results.csv ({len(df)} total matches)")
print(f" - filtered_professionals.csv ({len(filtered_df)} high-profile matches)")

# Show summary statistics
print(f"\nSummary:")
print(f"Total profiles found: {df['linkedin_url'].notna().sum()}")
print(f"High-profile matches: {len(filtered_df)}")
if len(filtered_df) > 0:
    print("\nSample high-profile matches:")
    for idx, row in filtered_df.head(3).iterrows():
        print(f"  - {row['name']}: {row['title']}")