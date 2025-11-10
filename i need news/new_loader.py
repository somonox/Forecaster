# new_loader.py
from __future__ import annotations
import asyncio
from typing import List, Optional, Dict
from urllib.parse import urlparse
import re
import html as htmlmod

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
        self.domains = domains
        self.keyword = keyword
        self.start_date = start_date
        self.end_date = end_date
        self.country = country
        self.language = language
        self.ua = ua
        self.debug = debug

    # -------------------------------------------------
    # GDELT에서 기사 메타데이터 수집 (동기)
    # -------------------------------------------------
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

    # -------------------------------------------------
    # HTML 파싱을 스레드 풀로 넘김 (CPU 병렬화)
    # -------------------------------------------------
    async def _parse_html_and_title(
        self, html: str, link: str
    ) -> (Optional[str], Optional[str], str):
        """
        무거운 HTML 파싱 (trafilatura + BeautifulSoup)을
        스레드 풀에서 실행해서 이벤트 루프 블로킹을 피한다.
        """

        def _work():
            # 본문 추출
            clean = extract_main_text(html, url=link)

            # 제목 추출
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

    # -------------------------------------------------
    # 단일 기사 비동기 요청
    # -------------------------------------------------
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
                # HTML이 아닌 경우는 본문 파싱 생략
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

    # -------------------------------------------------
    # 전체 기사 로딩 (비동기)
    # -------------------------------------------------
    async def load_news_async(
        self,
        max_concurrency: int = 10,
        timeout: float = 15.0,
        return_dataframe: bool = False,
    ):
        """
        1) GDELT에서 링크 수집(동기)
        2) httpx.AsyncClient로 병렬 크롤링
        3) list[dict] (기본) 또는 pandas.DataFrame 반환
        """
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

        # 메타 정보와 병합
        meta_map = {it["url"]: it for it in items}
        merged: List[Dict[str, Optional[str]]] = []
        for rec in results:
            base = meta_map.get(rec["url"], {})
            clean_text = rec.get("clean_text")
            if clean_text is None:
                continue  # 본문 없으면 스킵
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
