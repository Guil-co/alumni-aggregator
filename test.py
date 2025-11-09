import requests, json

url = "https://www.essecalumni.com/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=1"
data = requests.get(url, timeout=20).json()
print(json.dumps(data, indent=2)[:20000])
