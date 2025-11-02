# sector_with_yfinance.py
import time
import json
from pathlib import Path
from typing import Dict, Optional, List
import pandas as pd
import yfinance as yf

# 1) CUSIP -> Ticker 매핑 (레포 CSV)
def load_cusip_map(csv_path: str = "Stock_Data/CUSIP.csv") -> Dict[str, str]:
    df = pd.read_csv(csv_path, dtype=str)
    df["cusip"] = df["cusip"].str.strip()
    df["symbol"] = df["symbol"].str.upper().str.strip()
    return dict(zip(df["cusip"], df["symbol"]))

# 2) yfinance 섹터/산업 가져오기 (로컬 캐시 + 재시도)
class YFSectorFetcher:
    def __init__(self, cache_file: str = ".yf_sector_cache.json", sleep=0.2):
        self.cache_path = Path(cache_file)
        self.cache: Dict[str, Dict[str, Optional[str]]] = self._load()
        self.sleep = sleep  # 레이트 리밋 완화

    def _load(self) -> Dict[str, Dict[str, Optional[str]]]:
        if self.cache_path.exists():
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        return {}

    def _save(self):
        tmp = self.cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.cache, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.cache_path)

    def get_sector_industry(self, ticker: str, retries: int = 3) -> Dict[str, Optional[str]]:
        t = ticker.upper()
        if t in self.cache:
            return self.cache[t]
        last_err = None
        for _ in range(retries):
            try:
                info = yf.Ticker(t).get_info()  # yfinance 0.2+ 권장
                sector = info.get("sector")
                industry = info.get("industry")
                self.cache[t] = {"sector": sector, "industry": industry}
                self._save()
                time.sleep(self.sleep)
                return self.cache[t]
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        # 실패 시 캐시에 None으로 기록(다음에 재시도 가능)
        self.cache[t] = {"sector": None, "industry": None, "error": str(last_err) if last_err else None}
        self._save()
        return self.cache[t]

# 3) GICS 섹터 문자열 → 코드 매핑
GICS_SECTOR_CODE = {
    "Basic Materials": 15,
    "Communication Services": 50,
    "Consumer Cyclical": 25,
    "Consumer Defensive": 30,
    "Energy": 10,
    "Financial Services": 40,
    "Healthcare": 35,
    "Industrials": 20,
    "Real Estate": 60,
    "Technology": 45,
    "Utilities": 55,
}
def normalize_gics_code(sector_name: Optional[str]) -> Optional[int]:
    if not sector_name:
        return None
    return GICS_SECTOR_CODE.get(sector_name.strip())

# 4) Stock 리스트에 섹터/산업/코드 채우기
def enrich_stocks_with_yf(stocks: List["Stock"], cusip_to_ticker: Dict[str, str]):
    fetcher = YFSectorFetcher()
    for s in stocks:
        print(f"Enriching CUSIP: {s.cusip} / Issuer: {s.issuer}")
        if getattr(s, "sector", None) and getattr(s, "gics_sector_code", None):
            continue
        ticker = cusip_to_ticker.get(s.cusip)
        if not ticker:
            s.sector = getattr(s, "sector", None) or "Unknown"
            s.industry = getattr(s, "industry", None) or None
            s.gics_sector_code = getattr(s, "gics_sector_code", None) or None
            continue
        meta = fetcher.get_sector_industry(ticker)
        sector = meta.get("sector")
        industry = meta.get("industry")
        s.sector = sector or getattr(s, "sector", None) or "Unknown"
        s.industry = industry or getattr(s, "industry", None)
        s.gics_sector_code = normalize_gics_code(sector) or getattr(s, "gics_sector_code", None)
        print(f"  -> Sector: {s.sector}, Industry: {s.industry}, GICS Code: {s.gics_sector_code}")
