from __future__ import annotations
import asyncio
from typing import List, Optional, Dict
from urllib.parse import urlparse
import re, html as htmlmod

import httpx
from fallback_cache import cached_get
from gdeltdoc import GdeltDoc, Filters
from bs4 import BeautifulSoup
from readability.readability import Document
import trafilatura
import pandas as pd

# -------------------------------------------------
# 본문 정제 유틸 (동기) — 기존 로직 재사용
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
        html_str, url=url,
        include_comments=False, include_tables=False,
        with_metadata=False, favor_recall=False, no_fallback=True
    )
    if txt:
        return _normalize_text(txt)
    # 2) readability
    try:
        doc = Document(html_str)
        summary_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(summary_html, "lxml")
        for tag in soup(["script", "style", "noscript", "template", "iframe", "svg", "math"]):
            tag.decompose()
        text = _normalize_text(soup.get_text(separator="\n"))
        if not _is_minimal(text):
            return text
    except Exception:
        pass
    # 3) 태그 전체 제거
    soup = BeautifulSoup(html_str, "lxml")
    for tag in soup(["script", "style", "noscript", "template", "iframe", "svg", "math"]):
        tag.decompose()
    text = _normalize_text(soup.get_text(separator="\n"))
    return None if _is_minimal(text) else text


# -------------------------------------------------
# 비동기 로더 (httpx)
# -------------------------------------------------
DEFAULT_UA = "somonox"  # SEC/GDELT 등은 User-Agent 명시 권장

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
    ):
        self.domains = domains
        self.keyword = keyword
        self.start_date = start_date
        self.end_date = end_date
        self.country = country
        self.language = language
        self.ua = ua

    def __get_news_links(self) -> List[Dict[str, str]]:
        """GDELT에서 기사 목록을 받아 URL/제목/도메인 등 최소 메타를 추출(동기)."""
        f = Filters(
            keyword=self.keyword,
            start_date=self.start_date,  # 'YYYY-MM-DD' 또는 datetime
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
        out = []
        for _, row in articles.iterrows():
            url = row.get("url")
            if not url:
                continue
            out.append({
                "url": url,
                "title": row.get("title") if "title" in cols else None,
                "domain": row.get("domain") if "domain" in cols else urlparse(url).netloc,
                "seendate": row.get("seendate") if "seendate" in cols else None,
                "source": row.get("sourceCommonName") if "sourceCommonName" in cols else None,
            })
        return out

    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        link: str,
        timeout: float = 15.0,
        max_retries: int = 3,
        backoff_base: float = 1.0,
    ) -> Dict[str, Optional[str]]:
        """단일 URL 비동기 요청 + 본문 추출. 재시도/백오프 포함."""
        print(f"Fetching: {link}")
        for attempt in range(max_retries):
            try:
                r = await client.get(link, timeout=timeout)
                if r.status_code != 200:
                    # 4xx/5xx는 재시도 가치가 있는 5xx만 백오프
                    if 500 <= r.status_code < 600 and attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (2 ** attempt))
                        continue
                    return {"url": link, "raw_html": None, "clean_text": None, "title": None, "domain": urlparse(link).netloc}

                ct = (r.headers.get("Content-Type") or "").lower()
                text = r.text
                if "html" not in ct and not text.lstrip().startswith("<"):
                    # JSON 등 비-HTML 응답
                    return {"url": link, "raw_html": text, "clean_text": None, "title": None, "domain": urlparse(link).netloc}

                clean = extract_main_text(text, url=link)

                title = None
                try:
                    soup = BeautifulSoup(text, "lxml")
                    if soup.title and soup.title.string:
                        title = _normalize_text(soup.title.string)
                except Exception:
                    pass

                return {
                    "url": link,
                    "raw_html": text,
                    "clean_text": clean,
                    "title": title,
                    "domain": urlparse(link).netloc,
                }

            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {"url": link, "raw_html": None, "clean_text": None, "title": None, "domain": urlparse(link).netloc}
            except httpx.HTTPError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {"url": link, "raw_html": None, "clean_text": None, "title": None, "domain": urlparse(link).netloc}
            except Exception:
                # 알 수 없는 예외: 마지막 시도 아니면 백오프 후 재시도
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff_base * (2 ** attempt))
                    continue
                return {"url": link, "raw_html": None, "clean_text": None, "title": None, "domain": urlparse(link).netloc}

    async def load_news_async(
        self,
        max_concurrency: int = 10,
        timeout: float = 15.0,
        return_dataframe: bool = True,
    ):
        """
        1) GDELT에서 링크 수집(동기)
        2) httpx.AsyncClient로 병렬 크롤링
        3) list[dict] 또는 pandas.DataFrame 반환
        """
        items = self.__get_news_links()
        if not items:
            return pd.DataFrame([]) if return_dataframe else []

        urls = [it["url"] for it in items]

        limits = httpx.Limits(max_connections=max_concurrency, max_keepalive_connections=max_concurrency)
        headers = {
            "User-Agent": self.ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        }

        sem = asyncio.Semaphore(max_concurrency)

        async with httpx.AsyncClient(headers=headers, limits=limits, follow_redirects=True, timeout=timeout) as client:
            async def _bounded_fetch(u: str):
                async with sem:
                    return await self._fetch_one(client, u, timeout=timeout)

            results = await asyncio.gather(*[_bounded_fetch(u) for u in urls])

        # 메타 병합
        merged: List[Dict[str, Optional[str]]] = []
        meta_map = {it["url"]: it for it in items}
        for rec in results:
            base = meta_map.get(rec["url"], {})
            merged.append({
                "url": rec.get("url"),
                "domain": rec.get("domain") or base.get("domain"),
                "title": rec.get("title") or base.get("title"),
                "seendate": base.get("seendate"),
                "source": base.get("source"),
                "raw_html": rec.get("raw_html"),
                "clean_text": rec.get("clean_text"),
            })

        if return_dataframe:
            df = pd.DataFrame(merged)
            df["clean_len"] = df["clean_text"].apply(lambda x: len(x) if isinstance(x, str) else 0)
            return df
        return merged

    # 편의를 위한 동기 래퍼
    def load_news(self, **kwargs):
        return asyncio.run(self.load_news_async(**kwargs))
