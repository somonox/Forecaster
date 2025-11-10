import time
from new_loader import AsyncNewsLoader
import json

# All-sector (English)
ALL_SECTOR_DOMAINS = [
    "reuters.com",
    "apnews.com",
    "bbc.com",
    "theguardian.com",
    "cnbc.com",
]

ALL_SECTOR_DOMAINS2 = [
    "marketwatch.com",
    "finance.yahoo.com",
    "news.yahoo.com",
    "forbes.com",
    "fortune.com",
    "investing.com",
]

# Energy / Utilities
ENERGY_DOMAINS = [
    "oilprice.com",
    "rigzone.com",
]
UTILITIES_DOMAINS = [
    "utilitydive.com",
    "powermag.com",
]

# Materials / Mining / Chemicals
MATERIALS_DOMAINS = [
    "mining.com",
    "azom.com"
]
CHEMICALS_DOMAINS = [
    "cen.acs.org",
    "nytimes.com"
]
MINING_DOMAINS = [
    "mining.com",  # (중복 허용 시 유지, 아니면 제거)
    "nytimes.com"
]

# Industrials / Logistics / Manufacturing
INDUSTRIALS_DOMAINS = [
    "manufacturing.net",
    "nytimes.com"
]
LOGISTICS_DOMAINS = [
    "freightwaves.com",
    "nytimes.com"
]
MANUFACTURING_DOMAINS = [
    "manufacturing.net",
    "nytimes.com"
]

# Consumer (Discretionary / Staples)
CONSUMER_DISCRETIONARY_DOMAINS = [
    "retaildive.com",
    "nytimes.com"
]
CONSUMER_STAPLES_DOMAINS = [
    "grocerydive.com",
    "nytimes.com"
]

# Communication Services (Media / Telecom)
MEDIA_DOMAINS = [
    "hollywoodreporter.com",
    "variety.com",
]
TELECOM_DOMAINS = [
    "lightreading.com",
    "nytimes.com"
]

# Technology
TECHNOLOGY_DOMAINS = [
    "techcrunch.com",
    "theverge.com",
    "arstechnica.com",
]

# Real Estate
REAL_ESTATE_DOMAINS = [
    "therealdeal.com",
    "housingwire.com",
]

# Financials
FINANCIALS_DOMAINS = [
    "nasdaq.com",
    "seekingalpha.com",
]

# (옵션) 전체 합치기 (중복 제거 + 정렬)
ALL_DOMAINS = {
                "finance": [ALL_SECTOR_DOMAINS, ALL_SECTOR_DOMAINS2, FINANCIALS_DOMAINS],
                "energy": [ENERGY_DOMAINS],
                "utilities": [UTILITIES_DOMAINS],
                "materials": [MATERIALS_DOMAINS],
                "chemicals": [CHEMICALS_DOMAINS],
                "mining": [MINING_DOMAINS],
                "industrials": [INDUSTRIALS_DOMAINS],
                "logistics": [LOGISTICS_DOMAINS],
                "manufacturing": [MANUFACTURING_DOMAINS],
                "consumer": [CONSUMER_DISCRETIONARY_DOMAINS, CONSUMER_STAPLES_DOMAINS],
                "communication": [TELECOM_DOMAINS],
                "media": [MEDIA_DOMAINS],
                "technology": [TECHNOLOGY_DOMAINS],
                "estate": [REAL_ESTATE_DOMAINS]
               }



news_dumps = open("news_dumps.json", "w", encoding="utf-8")
final_list = {}
manyhours = [
("2025-05-31", "2025-06-30"),
("2025-04-28", "2025-05-30"),
("2025-03-31", "2025-04-30"),
("2025-02-28", "2025-03-30"),
("2025-01-31", "2025-02-27"),
("2024-12-31", "2025-01-30"),
("2024-11-30", "2024-12-30"),
("2024-10-31", "2024-11-29"),
("2024-09-30", "2024-10-31"),
("2024-08-31", "2024-09-30"),
("2024-07-31", "2024-08-31"),
("2024-06-30", "2024-07-31"),
("2024-05-31", "2024-06-30"),
("2024-04-30", "2024-05-30"),
("2024-03-31", "2024-04-30"),
("2024-02-29", "2024-03-30"),
("2024-01-31", "2024-02-28"),
("2023-12-31", "2024-01-30"),
("2023-11-30", "2023-12-30"),
("2023-10-31", "2023-11-29"),
("2023-09-30", "2023-10-31"),
("2023-08-31", "2023-09-30"),
("2023-07-31", "2023-08-31"),
("2023-06-30", "2023-07-31"),
("2023-05-31", "2023-06-30"),
("2023-04-30", "2023-05-30"),
("2023-03-31", "2023-04-30"),
("2023-02-28", "2023-03-30"),
("2023-01-31", "2023-02-27"),
]

for i, j in ALL_DOMAINS.items():
    semifinal_list = []
    for k in j:
        for start_date, end_date in manyhours:
            print(f"Loading news for sector: {i} from domain: {k}")
            news_loader = AsyncNewsLoader(k, keyword=i, start_date=start_date, end_date=end_date)
            data = news_loader.load_news()
            semifinal_list.append(data)
    final_list[i] = semifinal_list
json.dump(final_list, news_dumps, indent=4)
news_dumps.close()
