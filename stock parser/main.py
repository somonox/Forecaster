#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import yfinance as yf


def load_config(path: str = "config.json") -> Dict[str, Any]:
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg.setdefault("interval", "1d")
    cfg.setdefault("auto_adjust", True)
    cfg.setdefault("wide_format", True)
    cfg.setdefault("fields", ["Open", "High", "Low", "Close", "Adj Close", "Volume"])
    return cfg


def ensure_fields(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
    # 일부 인터벌/옵션에서 "Adj Close"가 없을 수 있음 → 없으면 만들기
    for col in fields:
        if isinstance(df.columns, pd.MultiIndex):
            # download(wide) 형태: level=1에 필드들이 존재
            if col not in df.columns.get_level_values(1):
                # 티커별로 결측 컬럼 추가
                new_cols = []
                for tkr in sorted(set(df.columns.get_level_values(0))):
                    new_cols.append((tkr, col))
                for c in new_cols:
                    df[c] = pd.NA
        else:
            if col not in df.columns:
                df[col] = pd.NA
    return df


def to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    # yfinance.download는 기본 멀티인덱스 컬럼(티커, 필드)
    df_long = (
        df_wide.stack(level=0)  # 티커를 행으로
        .rename_axis(index=["Date", "Ticker"])
        .reset_index()
    )
    return df_long


def fetch_history(cfg: Dict[str, Any]) -> pd.DataFrame:
    tickers: List[str] = cfg["tickers"]
    start: Optional[str] = cfg.get("start")
    end: Optional[str] = cfg.get("end")
    period: Optional[str] = cfg.get("period")
    interval: str = cfg["interval"]
    auto_adjust: bool = cfg["auto_adjust"]
    want_fields: List[str] = cfg["fields"]
    wide_format: bool = cfg["wide_format"]

    if (start or end) and period:
        # start/end가 우선, period는 무시
        period = None

    # 멀티-티커 한 번에 다운로드(빠르고 일관적)
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        period=period,
        interval=interval,
        auto_adjust=auto_adjust,
        group_by="ticker",
        threads=True,
        progress=False,
    )

    # 분/시간봉은 거래시간 외 데이터가 없거나 제한될 수 있음
    if df.empty:
        raise RuntimeError("다운로드 결과가 비어 있습니다. 기간/interval/티커를 확인하세요.")

    # 필드 보정
    df = ensure_fields(df, want_fields)

    # wide_format 처리
    if wide_format:
        # 멀티컬럼에서 불필요 필드 제거 및 컬럼 정렬
        # df.columns → (Ticker, Field)
        # 원하는 필드만 유지
        keep = []
        for tkr, fld in df.columns:
            if fld in want_fields:
                keep.append((tkr, fld))
        df = df.loc[:, keep]
        # 필드 순서 맞추기
        # (티커, 필드) 순서 중 필드를 재정렬
        ordered_cols = []
        for tkr in sorted(set([c[0] for c in df.columns])):
            for fld in want_fields:
                col = (tkr, fld)
                if col in df.columns:
                    ordered_cols.append(col)
        df = df.reindex(columns=ordered_cols)
        return df
    else:
        # long 포맷으로 변환 후 필드 필터링
        if not isinstance(df.columns, pd.MultiIndex):
            # 단일 티커일 때 컬럼이 단일 인덱스일 수 있음 → 티커 이름 붙이기
            tkr = tickers[0] if len(tickers) == 1 else "TICKER"
            df.columns = pd.MultiIndex.from_product([[tkr], df.columns])
        df_long = to_long(df)
        # 원하는 필드만
        df_long = df_long[df_long["level_1"].isin(want_fields)]
        # level_1 → Field로 이름 바꾸기
        df_long = df_long.rename(columns={"level_1": "Field"})
        # 열 재배치
        cols = ["Date", "Ticker", "Field"] + [c for c in df_long.columns if c not in ["Date", "Ticker", "Field"]]
        df_long = df_long[cols]
        return df_long


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("config", nargs="?", default="config.json")
    ap.add_argument("--show", action="store_true", help="콘솔에 상위 20행 미리보기")
    args = ap.parse_args()

    cfg = load_config(args.config)
    df = fetch_history(cfg)

    output_csv = cfg.get("output_csv")
    if output_csv:
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, encoding="utf-8", index=True)
        print(f"Saved to: {Path(output_csv).resolve()}")

    if args.show:
        print(df.head(20).to_string())


if __name__ == "__main__":
    main()
