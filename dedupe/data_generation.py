import requests
import pandas as pd

query = """
SELECT DISTINCT ?company ?companyLabel ?alias ?description
WHERE {
  ?company wdt:P31 wd:Q4830453.
  OPTIONAL { ?company skos:altLabel ?alias. FILTER(LANG(?alias) = "en") }
  OPTIONAL { ?company schema:description ?description. FILTER(LANG(?description) = "en") }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 1000
"""

url = "https://query.wikidata.org/sparql"
headers = {"User-Agent": "DeduplicationTool/1.0"}
response = requests.get(url, params={"query": query, "format": "json"}, headers=headers)

# Parse results
data = response.json()["results"]["bindings"]
rows = []
for item in data:
    rows.append({
        "Label": item.get("companyLabel", {}).get("value", ""),
        "Alias": item.get("alias", {}).get("value", ""),
        "Description": item.get("description", {}).get("value", ""),
    })

# Save to CSV
df = pd.DataFrame(rows)
df.to_csv("data/wikidata_input.csv", index=False)
print("Data saved to data/wikidata_input.csv")
