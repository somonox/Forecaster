# fallback_cache.py (patched)
import os, json, time, hashlib
from pathlib import Path
from typing import Optional, Dict
import httpx  # hishel의 SyncCacheClient도 httpx.Client 호환

CACHE_DIR = Path(".fallback_cache")
CACHE_DIR.mkdir(exist_ok=True)

def _key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()

def _paths(key: str):
    return CACHE_DIR / f"{key}.meta.json", CACHE_DIR / f"{key}.data"

def _load(key: str):
    mp, dp = _paths(key)
    if not (mp.exists() and dp.exists()):
        return None
    try:
        meta = json.loads(mp.read_text(encoding="utf-8"))
        data = dp.read_bytes()
        return meta, data
    except Exception:
        return None

def _sanitize_headers(h: Dict[str, str]) -> Dict[str, str]:
    # httpx가 다시 디코딩하지 않도록 압축/전송 관련 헤더 제거
    out = dict(h)
    for k in ["Content-Encoding", "content-encoding",
              "Transfer-Encoding", "transfer-encoding",
              "Content-Length", "content-length"]:
        out.pop(k, None)
    return out

def _store(key: str, resp: httpx.Response):
    mp, dp = _paths(key)
    tmpm, tmpd = mp.with_suffix(".tmp"), dp.with_suffix(".tmp")

    # 이미 resp.content는 디코딩된 바이트일 수 있으므로 원본 헤더 그대로 저장하고
    # 재구성 시 sanitize 하겠습니다.
    meta = {
        "url": str(resp.request.url),
        "stored_at": int(time.time()),
        "headers": dict(resp.headers),
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "status": resp.status_code,
    }
    tmpd.write_bytes(resp.content)
    tmpm.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    os.replace(tmpd, dp); os.replace(tmpm, mp)

def _build_response_from_cache(client: httpx.Client, url: str, meta: Dict, data: bytes) -> httpx.Response:
    req = client.build_request("GET", url)
    headers = _sanitize_headers(meta.get("headers", {}))
    extensions = {"from_cache": True, "fallback_cache": True}
    return httpx.Response(
        status_code=200,
        headers=headers,
        content=data,
        request=req,
        extensions=extensions,
    )

def cached_get(client: httpx.Client, url: str, ttl: Optional[int] = None, extra_headers: Optional[Dict[str,str]] = None) -> httpx.Response:
    """
    - 서버가 Cache-Control 지시자를 안 줄 때 Hishel이 저장하지 않아도, 파일 폴백으로 캐시
    - ETag/Last-Modified가 있으면 조건부 요청(304) 사용
    - ttl(초)을 넘긴 캐시는 네트워크 실패시에만 사용 (강제 사용 원하면 ttl=None)
    """
    key = _key(url)
    loaded = _load(key)
    cond_headers = dict(extra_headers or {})

    if loaded:
        meta, data = loaded
        if meta.get("etag"):
            cond_headers["If-None-Match"] = meta["etag"]
        if meta.get("last_modified"):
            cond_headers["If-Modified-Since"] = meta["last_modified"]

        try:
            resp = client.get(url, headers=cond_headers)
            if resp.status_code == 304:
                # 변경 없음 → 캐시 사용 (압축 헤더 제거된 Response로 복원)
                return _build_response_from_cache(client, url, meta, data)
            else:
                if resp.status_code == 200:
                    _store(key, resp)
                return resp
        except Exception:
            # 네트워크 실패 → TTL 검증 후 캐시 사용
            if ttl is None or (int(time.time()) - meta.get("stored_at", 0) <= ttl):
                return _build_response_from_cache(client, url, meta, data)
            raise
    else:
        resp = client.get(url, headers=extra_headers)
        if resp.status_code == 200:
            _store(key, resp)
        return resp
