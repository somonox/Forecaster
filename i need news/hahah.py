from __future__ import annotations
import asyncio
from typing import List, Optional, Dict
from urllib.parse import urlparse
import re
import html as htmlmod
import time
import json

import httpx
from gdeltdoc import GdeltDoc, Filters
from bs4 import BeautifulSoup
from readability.readability import Document
import trafilatura
import pandas as pd

# -------------------------------------------------
# 본문 정제 유틸 (동기)
# -------------------------------------------------
_WS_RE = re.compile(r"[ \t\u00A0\u200b\u200c\u200d]+")
_NL_RE = re.compile(r"\n{3,}")
_CTRL_RE = re.compile(r"[\u0000-\u0008\u000B-\u000C\u000E-\u001F]+")


def _normalize_text(text: str) -> str:
    text = htmlmod.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _CTRL_RE.sub("", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = _WS_RE.sub(" ", text)
    text = _NL_RE.sub("\n\n", text).strip()
    return text


def _is_minimal(text: Optional[str], min_chars: int = 200) -> bool:
    return (text is None) or (len(text.strip()) < min_chars)


def extract_main_text(html_str: str, url: Optional[str] = None) -> Optional[str]:
    # 1) trafilatura
    txt = trafilatura.extract(
        html_str,
        url=url,
        include_comments=False,
        include_tables=False,
        with_metadata=False,
        favor_recall=False,
        no_fallback=True,
    )
    if txt:
        return _normalize_text(txt)

    # 2) readability
    try:
        doc = Document(html_str)
        summary_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(summary_html, "lxml")
        for tag in soup(
            ["script", "style", "noscript", "template", "iframe", "svg", "math"]
        ):
            tag.decompose()
        text = _normalize_text(soup.get_text(separator="\n"))
        if not _is_minimal(text):
            return text
    except Exception:
        pass

    # 3) 태그 전체 제거
    soup = BeautifulSoup(html_str, "lxml")
    for tag in soup(
        ["script", "style", "noscript", "template", "iframe", "svg", "math"]
    ):
        tag.decompose()
    text = _normalize_text(soup.get_text(separator="\n"))
    return None if _is_minimal(text) else text


# -------------------------------------------------
# 비동기 뉴스 로더
# -------------------------------------------------
DEFAULT_UA = "somonox"


class AsyncNewsLoader:
    def __init__(
        self,
        domains: List[str],
        keyword: str,
        start_date: str,
        end_date: str,
        country: str = "US",
        language: str = "English",
        ua: str = DEFAULT_UA,
        debug: bool = False,
    ):
        self.domains = domains              # 반드시 "짧은" 도메인 리스트만!
        self.keyword = keyword
        self.start_date = start_date
        self.end_date = end_date
        self.country = country
        self.language = language
        self.ua = ua
        self.debug = debug

    # GDELT 메타데이터 조회 (동기)
    def __get_news_links(self) -> List[Dict[str, str]]:
        f = Filters(
            keyword=self.keyword,
            start_date=self.start_date,
            end_date=self.end_date,
            domain=self.domains,
            country=self.country,
            language=self.language,
        )
        gd = GdeltDoc()
        articles = gd.article_search(f)  # pandas.DataFrame
        if articles is None or len(articles) == 0:
            return []

        cols = set(articles.columns)
        out: List[Dict[str, str]] = []
        for _, row in articles.iterrows():
            url = row.get("url")
            if not url:
                continue
            out.append(
                {
                    "url": url,
                    "title": row.get("title") if "title" in cols else None,
                    "domain": row.get("domain")
                    if "domain" in cols
                    else urlparse(url).netloc,
                    "seendate": row.get("seendate") if "seendate" in cols else None,
                    "source": row.get("sourceCommonName")
                    if "sourceCommonName" in cols
                    else None,
                }
            )
        return out

    # HTML 파싱을 스레드 풀로 넘김 (CPU 병렬화)
    async def _parse_html_and_title(
        self, html: str, link: str
    ) -> (Optional[str], Optional[str], str):
        def _work():
            clean = extract_main_text(html, url=link)

            title = None
            try:
                soup = BeautifulSoup(html, "lxml")
                if soup.title and soup.title.string:
                    title = _normalize_text(soup.title.string)
            except Exception:
                pass

            domain = urlparse(link).netloc
            return clean, title, domain

        return await asyncio.to_thread(_work)

    # 단일 기사 요청
    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        link: str,
        timeout: float = 15.0,
        max_retries: int = 3,
        backoff_base: float = 1.0,
    ) -> Dict[str, Optional[str]]:
        if self.debug:
            print(f"Fetching: {link}")

        for attempt in range(max_retries):
            try:
                r = await client.get(link, timeout=timeout)
                if r.status_code != 200:
                    if 500 <= r.status_code < 600 and attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (2 ** attempt))
                        continue
                    return {
                        "url": link,
                        "clean_text": None,
                        "title": None,
                        "domain": urlparse(link).netloc,
                    }

                ct = (r.headers.get("Content-Type") or "").lower()
                text = r.text
                if "html" not in ct and not text.lstrip().startswith("<"):
                    return {
                        "url": link,
                        "clean_text": None,
                        "title": None,
                        "domain": urlparse(link).netloc,
                    }

                clean, title, domain = await self._parse_html_and_title(text, link)

                return {
                    "url": link,
                    "clean_text": clean,
                    "title": title,
                    "domain": domain,
                }

            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {
                    "url": link,
                    "clean_text": None,
                    "title": None,
                    "domain": urlparse(link).netloc,
                }
            except httpx.HTTPError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {
                    "url": link,
                    "clean_text": None,
                    "title": None,
                    "domain": urlparse(link).netloc,
                }
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {
                    "url": link,
                    "clean_text": None,
                    "title": None,
                    "domain": urlparse(link).netloc,
                }

    # 전체 기사 로딩 (비동기)
    async def load_news_async(
        self,
        max_concurrency: int = 10,
        timeout: float = 15.0,
        return_dataframe: bool = False,
    ):
        items = self.__get_news_links()
        if not items:
            return [] if not return_dataframe else pd.DataFrame([])

        urls = [it["url"] for it in items]

        limits = httpx.Limits(
            max_connections=max_concurrency,
            max_keepalive_connections=max_concurrency,
        )
        headers = {
            "User-Agent": self.ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        }

        async with httpx.AsyncClient(
            headers=headers,
            limits=limits,
            follow_redirects=True,
            timeout=timeout,
        ) as client:
            tasks = [self._fetch_one(client, u, timeout=timeout) for u in urls]
            results = await asyncio.gather(*tasks)

        meta_map = {it["url"]: it for it in items}
        merged: List[Dict[str, Optional[str]]] = []
        for rec in results:
            base = meta_map.get(rec["url"], {})
            clean_text = rec.get("clean_text")
            if clean_text is None:
                continue
            obj = {
                "url": rec.get("url"),
                "domain": rec.get("domain") or base.get("domain"),
                "title": rec.get("title") or base.get("title"),
                "seendate": base.get("seendate"),
                "source": base.get("source"),
                "clean_text": clean_text,
                "clean_len": len(clean_text) if isinstance(clean_text, str) else 0,
            }
            merged.append(obj)

        if return_dataframe:
            return pd.DataFrame(merged)

        return merged

    # 동기 래퍼
    def load_news(self, **kwargs):
        return asyncio.run(self.load_news_async(**kwargs))


# -------------------------------------------------
# 섹터/도메인/기간 설정
# -------------------------------------------------
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
    "azom.com",
]
CHEMICALS_DOMAINS = [
    "cen.acs.org",
    "nytimes.com",
]
MINING_DOMAINS = [
    "mining.com",
    "nytimes.com",
]

# Industrials / Logistics / Manufacturing
INDUSTRIALS_DOMAINS = [
    "manufacturing.net",
    "nytimes.com",
]
LOGISTICS_DOMAINS = [
    "freightwaves.com",
    "nytimes.com",
]
MANUFACTURING_DOMAINS = [
    "manufacturing.net",
    "nytimes.com",
]

# Consumer (Discretionary / Staples)
CONSUMER_DISCRETIONARY_DOMAINS = [
    "retaildive.com",
    "nytimes.com",
]
CONSUMER_STAPLES_DOMAINS = [
    "grocerydive.com",
    "nytimes.com",
]

# Communication Services (Media / Telecom)
MEDIA_DOMAINS = [
    "hollywoodreporter.com",
    "variety.com",
]
TELECOM_DOMAINS = [
    "lightreading.com",
    "nytimes.com",
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

# 섹터별 도메인 그룹
# ➜ 여기서는 "도메인 그룹(list)" 단위로 GDELT에 보내기 때문에
#    쿼리 길이가 너무 길어지지 않음.
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
    "estate": [REAL_ESTATE_DOMAINS],
}

# 수집 기간
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


# -------------------------------------------------
# 메인 실행
# -------------------------------------------------
def main(
    output_path: str = "news_dumps.json",
    max_concurrency: int = 10,
    timeout: float = 15.0,
):
    start_all = time.time()
    final_list: Dict[str, List] = {}

    for sector, domain_groups in ALL_DOMAINS.items():
        print(f"\n=== Sector: {sector} ===")
        semifinal_list = []

        # ✅ 섹터가 같더라도 "도메인 그룹" 단위로 분리해서 요청
        for domains in domain_groups:
            for start_date, end_date in manyhours:
                print(
                    f"  -> Loading news for sector '{sector}' "
                    f"from domains: {domains} "
                    f"({start_date} ~ {end_date})"
                )

                loader = AsyncNewsLoader(
                    domains=domains,
                    keyword=sector,
                    start_date=start_date,
                    end_date=end_date,
                )
                data = loader.load_news(
                    max_concurrency=max_concurrency,
                    timeout=timeout,
                    return_dataframe=False,
                )
                # 원래 코드와 동일하게 list만 쌓기
                semifinal_list.append(data)

        final_list[sector] = semifinal_list

    elapsed = time.time() - start_all
    print(f"\nTotal crawl time: {elapsed:.1f} seconds")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=4, ensure_ascii=False)

    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
