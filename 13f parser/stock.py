from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import List, Dict, Any, Iterable, Tuple, DefaultDict, Optional
from collections import defaultdict

def _to_decimal(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, (int, float, Decimal)):
        return Decimal(str(x))
    s = str(x).replace(",", "").strip()
    if s == "":
        return Decimal("0")
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")

def _to_int(x) -> int:
    if x is None:
        return 0
    if isinstance(x, (int,)):
        return int(x)
    s = str(x).replace(",", "").strip()
    return int(float(s)) if s else 0

def _parse_other_mgr(s: str | None) -> List[int]:
    if not s:
        return []
    return [int(t) for t in s.split(",") if t.strip().isdigit()]

@dataclass
class VotingAuthority:
    sole: int = 0
    shared: int = 0
    none: int = 0

    @classmethod
    def from_dict(cls, d: Dict[str, Any] | None) -> "VotingAuthority":
        d = d or {}
        # 키가 Sole/Shared/None 또는 소문자일 수 있어 유연 처리
        return cls(
            sole=_to_int(d.get("Sole") or d.get("sole")),
            shared=_to_int(d.get("Shared") or d.get("shared")),
            none=_to_int(d.get("None") or d.get("none")),
        )

    def merge(self, other: "VotingAuthority") -> "VotingAuthority":
        return VotingAuthority(
            sole=self.sole + other.sole,
            shared=self.shared + other.shared,
            none=self.none + other.none,
        )

@dataclass
class Stock:
    issuer: str
    title_of_class: str
    cusip: str
    value_usd: Decimal
    shares: int
    shares_type: str  # 보통 "SH"
    investment_discretion: str  # 예: "SOLE", "DFND"
    other_manager: List[int]
    voting: VotingAuthority
    sector: Optional[str] = None
    industry: Optional[str] = None
    gics_sector_code: Optional[int] = None

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "Stock":
        # 안전 파싱
        issuer = (row.get("nameOfIssuer") or "").strip()
        title = (row.get("titleOfClass") or "").strip()
        cusip = (row.get("cusip") or "").strip()

        value = _to_decimal(row.get("value"))
        sh = row.get("shrsOrPrnAmt") or {}
        shares = _to_int(sh.get("sshPrnamt"))
        shares_type = (sh.get("sshPrnamtType") or "").strip().upper()

        inv_disc = (row.get("investmentDiscretion") or "").strip().upper()
        other_mgr = _parse_other_mgr(row.get("otherManager"))

        voting = VotingAuthority.from_dict(row.get("votingAuthority"))

        return cls(
            issuer=issuer,
            title_of_class=title,
            cusip=cusip,
            value_usd=value,
            shares=shares,
            shares_type=shares_type,
            investment_discretion=inv_disc,
            other_manager=other_mgr,
            voting=voting,
        )

    # “동일 항목”을 판단하는 키 (원하는 기준으로 조정 가능)
    def key(self) -> Tuple[str, str, str]:
        return (self.issuer, self.title_of_class, self.cusip)

    # 동종 항목 병합 (가치/수량/의결권 합산; 투자재량/타매니저는 합집합 느낌)
    def merge(self, other: "Stock") -> "Stock":
        assert self.key() == other.key(), "Different positions cannot be merged"
        # investment_discretion: 서로 다르면 DFND 우선 등 규칙이 필요할 수 있음 -> 간단 병합
        inv_disc = self.investment_discretion if self.investment_discretion == other.investment_discretion else "DFND"
        # other_manager: 유니크 정렬
        om_set = sorted(set(self.other_manager + other.other_manager))
        return Stock(
            issuer=self.issuer,
            title_of_class=self.title_of_class,
            cusip=self.cusip,
            value_usd=(self.value_usd + other.value_usd),
            shares=(self.shares + other.shares),
            shares_type=self.shares_type or other.shares_type,
            investment_discretion=inv_disc,
            other_manager=om_set,
            voting=self.voting.merge(other.voting),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issuer": self.issuer,
            "titleOfClass": self.title_of_class,
            "cusip": self.cusip,
            # 값은 필요에 따라 문자열 또는 숫자로 바꿀 수 있음
            "valueUSD": str(self.value_usd.quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
            "shares": int(self.shares),
            "sharesType": self.shares_type,
            "investmentDiscretion": self.investment_discretion,
            "otherManager": ",".join(map(str, self.other_manager)) if self.other_manager else "",
            "votingAuthority": {
                "sole": int(self.voting.sole),
                "shared": int(self.voting.shared),
                "none": int(self.voting.none),
            },
            # enrichment 필드 포함
            "sector": self.sector,
            "industry": self.industry,
            "gicsSectorCode": self.gics_sector_code,
        }

# -------- 파서/집계 유틸 --------

def parse_info_table(rows: Iterable[Dict[str, Any]]) -> List[Stock]:
    """ Infotable(JSON 리스트) -> Stock 객체 리스트 """
    return [Stock.from_dict(r) for r in rows]

def group_and_merge(stocks: Iterable[Stock]) -> List[Stock]:
    """ 동일 (issuer, class, CUSIP)끼리 병합 """
    groups: DefaultDict[Tuple[str, str, str], Stock] = defaultdict(lambda: None)  # type: ignore
    for s in stocks:
        k = s.key()
        if groups[k] is None:
            groups[k] = s
        else:
            groups[k] = groups[k].merge(s)
    return list(groups.values())

def total_value(stocks: Iterable[Stock]) -> Decimal:
    return sum((s.value_usd for s in stocks), Decimal("0"))

def to_csv_rows(stocks: Iterable[Stock]) -> List[List[str]]:
    header = ["issuer", "titleOfClass", "CUSIP", "valueUSD", "shares", "sharesType", "investmentDiscretion", "otherManager", "votingSole", "votingShared", "votingNone"]
    data = [header]
    for s in stocks:
        data.append([
            s.issuer,
            s.title_of_class,
            s.cusip,
            str(s.value_usd),
            str(s.shares),
            s.shares_type,
            s.investment_discretion,
            ",".join(map(str, s.other_manager)) if s.other_manager else "",
            str(s.voting.sole),
            str(s.voting.shared),
            str(s.voting.none),
        ])
    return data
