# pip install hishel[httpx]
import json
from typing import List, Dict

import xmltodict
from bs4 import BeautifulSoup
import httpx

import stock
import logging
import os
import time
from fallback_cache import cached_get
from love_cusip import *
import decimal, datetime
import dataclasses

BASE_URL = "https://www.sec.gov"
BASE_DATA_URL = "https://data.sec.gov"

# 공통 헤더 (SEC 권고: UA에 앱/연락 이메일 명시)

UA = "somonox hungmin090929@gmail.com"

client = httpx.Client(headers={"User-Agent": UA})

def make_serializable(obj):
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    if isinstance(obj, (list, tuple, set)):
        return [make_serializable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    if hasattr(obj, "to_dict"):
        return make_serializable(obj.to_dict())
    if hasattr(obj, "__dict__"):
        return make_serializable(vars(obj))
    # dataclass 지원 우선
    try:
        if dataclasses.is_dataclass(obj):
            return make_serializable(dataclasses.asdict(obj))
    except Exception:
        pass

    # to_dict가 있으면 시도하되 None/예외 시 __dict__로 폴백
    if hasattr(obj, "to_dict"):
        try:
            d = obj.to_dict()
            if d is not None:
                return make_serializable(d)
        except Exception:
            pass

    if hasattr(obj, "__dict__"):
        return make_serializable(vars(obj))
    return str(obj)

def get_company_info(cik: str):
    print(f"Fetching company info for CIK: {cik}")
    url = f"{BASE_DATA_URL}/submissions/CIK{cik}.json"
    r = cached_get(client, url)
    r.raise_for_status()
    return r.json()

def get_all_13f_links(cik: str):
    thirteenth_filing_url = f"{BASE_URL}/Archives/edgar/data/{{0}}/{{1}}"
    primDocs = []
    thirteenth_fillings_links = []
    fillings = get_company_info(cik)['filings']['recent']
    unpaddinged_cik = str(int(cik))

    # Get all 13F-HR primary documents
    for (index, form_name) in enumerate(fillings['form']):
        if form_name == '13F-HR':
            url = thirteenth_filing_url.format(
                unpaddinged_cik,
                fillings['accessionNumber'][index].replace('-', ''),
            )
            primDocs.append(url)

    for folder_url in primDocs:
        res = cached_get(client, folder_url)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        anchors = soup.find_all('a', recursive=True)
        for i, a in enumerate(anchors, 1):
            print(f"Processing {i}/{len(anchors)} links in {folder_url}")
            href = (a.get('href') or '')
            if href.endswith('.xml'):
                thirteenth_fillings_links.append(f"{BASE_URL}{href}")
                break

    return thirteenth_fillings_links

def get_all_13f(cik: str) -> List[Dict]:
    links = get_all_13f_links(cik)
    thirteenf_list = []
    for idx, link in enumerate(links, 1):
        print(f"Processing {idx}/{len(links)} links in 13F for CIK: {cik}")
        r = cached_get(client, link)
        r.raise_for_status()
        data = xmltodict.parse(r.content, attr_prefix='@', cdata_key='#text')
        thirteenf_list.append(data['informationTable']['infoTable'])
    return thirteenf_list

if __name__ == "__main__":
    # Berkshire Hathaway
    data = get_all_13f('0001067983')
    if not data:
        print("No 13F data found.")
    else:
        ttf_brk = data[0]
        stocks = stock.parse_info_table(ttf_brk)
        # with open("a.json", "w", encoding="utf-8") as f:
        #     f.write(json.dumps(make_serializable(stocks), indent=4, ensure_ascii=False))
        merged_stocks = stock.group_and_merge(stocks)
        total = stock.total_value(merged_stocks)
        cusip_map = load_cusip_map("CUSIP.csv")

        enrich_stocks_with_yf(merged_stocks, cusip_map)

        serializable = make_serializable(merged_stocks)

        for s in serializable[:10]:
            print(s)

        with open("ttf_brk.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(serializable, indent=4, ensure_ascii=False))
    

