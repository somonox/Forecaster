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
ALL_DOMAINS = sorted(set(
    ALL_SECTOR_DOMAINS
    + ENERGY_DOMAINS + UTILITIES_DOMAINS
    + MATERIALS_DOMAINS + CHEMICALS_DOMAINS + MINING_DOMAINS
    + INDUSTRIALS_DOMAINS + LOGISTICS_DOMAINS + MANUFACTURING_DOMAINS
    + CONSUMER_DISCRETIONARY_DOMAINS + CONSUMER_STAPLES_DOMAINS
    + MEDIA_DOMAINS + TELECOM_DOMAINS
    + TECHNOLOGY_DOMAINS
    + REAL_ESTATE_DOMAINS
    + FINANCIALS_DOMAINS
))

news_dumps = open("news_dumps.json", "w", encoding="utf-8")


news_loader = AsyncNewsLoader(ALL_SECTOR_DOMAINS, keyword="finance", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(ALL_SECTOR_DOMAINS2, keyword="finance", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(ENERGY_DOMAINS, keyword="energy", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(UTILITIES_DOMAINS, keyword="utilities", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(MATERIALS_DOMAINS, keyword="materials", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(CHEMICALS_DOMAINS, keyword="chemicals", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(MINING_DOMAINS, keyword="mining", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(INDUSTRIALS_DOMAINS, keyword="industrials", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(LOGISTICS_DOMAINS, keyword="logistics", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(MANUFACTURING_DOMAINS, keyword="manufacturing", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(CONSUMER_DISCRETIONARY_DOMAINS, keyword="consumer", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(CONSUMER_STAPLES_DOMAINS, keyword="consumer", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(MEDIA_DOMAINS, keyword="media", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(TELECOM_DOMAINS, keyword="telecom", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(TECHNOLOGY_DOMAINS, keyword="technology", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(REAL_ESTATE_DOMAINS, keyword="estate", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))
news_loader = AsyncNewsLoader(FINANCIALS_DOMAINS, keyword="finance", start_date="2025-03-31", end_date="2025-06-30")
data = news_loader.load_news()
news_dumps.write(json.dumps(data, indent=4))

news_dumps.close()
