from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, TypedDict
from bisect import bisect_right

import requests
import pandas as pd
from decimal import Decimal

try:
    from eth_utils import to_checksum_address
except Exception:  # pragma: no cover - optional dependency
    def to_checksum_address(addr: str) -> str:  # type: ignore
        return addr


SECONDS_PER_YEAR = 365 * 24 * 3600


def ray_to_percent(ray_value: str) -> float:
    """Convert Ray (27 decimals) string to percent (float)."""
    return float(Decimal(ray_value) / Decimal(10 ** 27) * 100)


def _base_url() -> str:
    return os.getenv("HYPERLEND_BASE_URL", "https://api.hyperlend.finance")


def fetch_markets(chain: str, base_url: Optional[str] = None) -> Dict[str, int]:
    """Thin wrapper to fetch only decimals map (reuses meta implementation)."""
    decimals_map, _name_map = fetch_markets_meta(chain, base_url)
    return decimals_map


def fetch_markets_meta(chain: str, base_url: Optional[str] = None) -> Tuple[Dict[str, int], Dict[str, str]]:
    """Fetch markets and return (decimals_map, name_map).

    name_map prefers 'symbol' if available, else 'name', else the checksummed address.
    """
    base = base_url or _base_url()
    url = f"{base}/data/markets"
    resp = requests.get(url, params={"chain": chain}, timeout=20)
    resp.raise_for_status()
    data = resp.json() or {}
    decimals_map: Dict[str, int] = {}
    name_map: Dict[str, str] = {}
    for r in data.get("reserves", []):
        ua_raw = r.get("underlyingAsset")
        if not ua_raw:
            continue
        try:
            ua = to_checksum_address(ua_raw)
        except Exception:
            ua = ua_raw
        dec = r.get("decimals")
        sym = r.get("symbol")
        nm = r.get("name")
        display = sym or nm or ua
        try:
            decimals_map[ua] = int(dec) if dec is not None else 18
        except Exception:
            decimals_map[ua] = 18
        name_map[ua] = str(display)
    return decimals_map, name_map


def fetch_interest_rate_history(chain: str, token: str, base_url: Optional[str] = None) -> List[dict]:
    """Fetch hourly interest rate history for a token.

    Returns a list of entries. Each entry includes 'timestamp' (ms) and a token-keyed object with
    'currentVariableBorrowRate' expressed in Ray.
    """
    base = base_url or _base_url()
    token_cs = to_checksum_address(token)
    url = f"{base}/data/interestRateHistory"
    resp = requests.get(url, params={"chain": chain, "token": token_cs}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_user_tx_history(chain: str, address: str, base_url: Optional[str] = None,
                          limit: int = 1000, max_pages: int = 10) -> List[dict]:
    """Fetch the user's transaction history (Borrow/Repay/etc.). Paginates until empty or max_pages.

    Returns a flat list of event dicts under 'data'. Each event contains e.g.:
      - blockNumber, timestamp (seconds), event ("Borrow" | "Repay" | ...), data{...}
    """
    base = base_url or _base_url()
    addr_cs = to_checksum_address(address)
    out: List[dict] = []
    for page in range(max_pages):
        skip = page * limit
        url = f"{base}/data/user/transactionHistory"
        resp = requests.get(url, params={"chain": chain, "address": addr_cs, "limit": limit, "skip": skip}, timeout=30)
        resp.raise_for_status()
        j = resp.json() or {}
        batch = j.get("data", [])
        if not batch:
            break
        out.extend(batch)
        if len(batch) < limit:
            break
    # sort by (blockNumber, logIndex) if present, else timestamp
    out.sort(key=lambda e: (e.get("blockNumber", 0), e.get("logIndex", 0), e.get("timestamp", 0)))
    return out


@dataclass
class PositionState:
    principal: float = 0.0  # outstanding principal in token units
    last_ts: Optional[int] = None  # seconds
    total_borrowed: float = 0.0
    total_repaid: float = 0.0
    total_interest_accrued: float = 0.0
    principal_time_seconds: float = 0.0  # sum of principal * dt over time for avg rate calc


class RateCurve(TypedDict):
    t: List[int]      # timestamps (sec), sorted ascending
    r: List[float]    # per-second rates aligned with t
    cum: List[float]  # cumulative integral of rate dt up to t[i]


def build_rate_curve(token: str, rate_history: List[dict]) -> RateCurve:
    """Build a piecewise-constant rate curve with cumulative integral for fast accrual queries."""
    token_cs = to_checksum_address(token)
    pts: List[Tuple[int, float]] = []
    for entry in rate_history:
        ts_ms = entry.get("timestamp")
        if ts_ms is None:
            continue
        pool = entry.get(token_cs)
        if not isinstance(pool, dict):
            continue
        v = pool.get("currentVariableBorrowRate")
        if v is None:
            continue
        apr = ray_to_percent(str(v)) / 100.0  # APR as fraction
        per_sec = apr / SECONDS_PER_YEAR
        pts.append((int(ts_ms // 1000), per_sec))
    pts.sort(key=lambda x: x[0])
    t: List[int] = [p[0] for p in pts]
    r: List[float] = [p[1] for p in pts]
    cum: List[float] = [0.0] * len(t)
    for i in range(1, len(t)):
        dt = t[i] - t[i - 1]
        if dt < 0:
            dt = 0
        cum[i] = cum[i - 1] + r[i - 1] * dt
    return {"t": t, "r": r, "cum": cum}

def _integral_at(curve: RateCurve, ts: int) -> float:
    """Integral of per-second rate from curve origin up to ts.

    If ts is before the first timestamp, extrapolate from the first timestamp using first rate.
    """
    t = curve.get("t", [])
    if not t:
        return 0.0
    r = curve["r"]
    cum = curve["cum"]
    idx = bisect_right(t, ts) - 1
    if idx < 0:
        return r[0] * (ts - t[0])
    return cum[idx] + r[idx] * (ts - t[idx])


def accrue_interest(principal: float, t0: int, t1: int, curve: RateCurve) -> float:
    """Accrue interest on principal from t0 to t1 using cumulative integral (O(log N))."""
    if principal <= 0 or t1 <= t0:
        return 0.0
    return principal * (_integral_at(curve, t1) - _integral_at(curve, t0))


def scale_amount(amount_wei_str: str, decimals: int) -> float:
    """Scale integer string amount by decimals -> float units."""
    q = Decimal(amount_wei_str)
    return float(q / (Decimal(10) ** decimals))


def analyze_loans(chain: str, address: str, base_url: Optional[str] = None,
                  token_filter: Optional[str] = None,
                  as_of_ts: Optional[int] = None) -> Tuple[pd.DataFrame, dict]:
    """Analyze borrow/repay history and compute per-event evolution and totals.

    Returns:
      - DataFrame with per-event evolution
      - Summary dict with totals
    """
    base = base_url or _base_url()
    addr_cs = to_checksum_address(address)

    try:
        decimals_map, name_map = fetch_markets_meta(chain, base)
    except Exception:
        # Fallback if meta fetch fails
        decimals_map = fetch_markets(chain, base)
        name_map = {}

    events = fetch_user_tx_history(chain, addr_cs, base)

    states: Dict[str, PositionState] = {}
    curves: Dict[str, RateCurve] = {}

    rows: List[dict] = []

    def ensure_curve(reserve: str) -> RateCurve:
        reserve_cs = to_checksum_address(reserve)
        if reserve_cs not in curves:
            hist = fetch_interest_rate_history(chain, reserve_cs, base)
            curves[reserve_cs] = build_rate_curve(reserve_cs, hist)
        return curves[reserve_cs]

    for ev in events:
        evt = ev.get("event")
        if evt not in ("Borrow", "Repay"):
            continue
        data = ev.get("data", {})
        reserve = data.get("reserve")
        if not reserve:
            continue
        reserve_cs = to_checksum_address(reserve)
        if token_filter and to_checksum_address(token_filter) != reserve_cs:
            continue

        ts = int(ev.get("timestamp", 0))  # seconds
        blk = ev.get("blockNumber", None)
        logi = ev.get("logIndex", None)

        # Amount and decimals
        dec = decimals_map.get(reserve_cs, 18)
        amount = scale_amount(str(data.get("amount", "0")), dec)

        # Initialize state
        st = states.setdefault(reserve_cs, PositionState())

        # Accrue interest from last_ts to current ts on existing principal
        accrued = 0.0
        if st.last_ts is not None and st.principal > 0:
            curve = ensure_curve(reserve_cs)
            accrued = accrue_interest(st.principal, st.last_ts, ts, curve)
            # accumulate principal*time for average rate calculation
            st.principal_time_seconds += max(0, ts - st.last_ts) * st.principal

        principal_before = st.principal
        principal_after = principal_before

        if evt == "Borrow":
            st.total_borrowed += amount
            principal_after = principal_before + amount
        elif evt == "Repay":
            st.total_repaid += amount
            principal_after = max(0.0, principal_before + accrued - amount)

        # Update state for next step
        st.total_interest_accrued += accrued
        st.principal = principal_after
        st.last_ts = ts

        rows.append({
            "reserve": reserve_cs,
            "blockNumber": blk,
            "logIndex": logi,
            "timestamp": ts,
            "event": evt,
            "amount": amount,
            "principal_before": principal_before,
            "accrued_interest_since_last": accrued,
            "principal_after": principal_after,
            "total_borrowed_so_far": st.total_borrowed,
            "total_repaid_so_far": st.total_repaid,
            "total_interest_accrued_so_far": st.total_interest_accrued,
        })

    # after processing on-chain events, accrue interest from last event to as_of_ts (default: now)
    if as_of_ts is None:
        as_of_ts = int(time.time())

    for reserve_cs, st in states.items():
        if st.last_ts is None or st.principal <= 0:
            continue
        if as_of_ts <= st.last_ts:
            continue
        curve = ensure_curve(reserve_cs)
        accrued_now = accrue_interest(st.principal, st.last_ts, as_of_ts, curve)
        # accumulate principal*time through the accrual-to-now interval
        st.principal_time_seconds += max(0, as_of_ts - st.last_ts) * st.principal
        principal_before = st.principal
        st.total_interest_accrued += accrued_now
        st.principal = principal_before + accrued_now
        # record synthetic accrual step
        rows.append({
            "reserve": reserve_cs,
            "blockNumber": None,
            "logIndex": None,
            "timestamp": as_of_ts,
            "event": "Accrual",
            "amount": 0.0,
            "principal_before": principal_before,
            "accrued_interest_since_last": accrued_now,
            "principal_after": st.principal,
            "total_borrowed_so_far": st.total_borrowed,
            "total_repaid_so_far": st.total_repaid,
            "total_interest_accrued_so_far": st.total_interest_accrued,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
        df = df.sort_values(["reserve", "blockNumber", "logIndex"], na_position="last").reset_index(drop=True)

    # build summary across reserves (and per-reserve)
    summary_per_reserve = {}
    total_borrowed = 0.0
    total_repaid = 0.0
    total_interest_accrued = 0.0
    outstanding = 0.0
    total_principal_time_seconds = 0.0
    for r, st in states.items():
        debt_name = name_map.get(r, r)
        # compute average APR and APY per reserve
        if st.principal_time_seconds > 0:
            avg_apr = (st.total_interest_accrued * SECONDS_PER_YEAR) / st.principal_time_seconds
            avg_apy = (1.0 + (avg_apr / SECONDS_PER_YEAR)) ** SECONDS_PER_YEAR - 1.0
        else:
            avg_apr = 0.0
            avg_apy = 0.0
        summary_per_reserve[r] = {
            "debt_name": debt_name,
            "borrowed": st.total_borrowed,
            "repaid": st.total_repaid,
            "interest_accrued": st.total_interest_accrued,
            "outstanding_principal": st.principal,
            "average_apr": avg_apr * 100,
            "average_apy": avg_apy * 100,
        }
        total_borrowed += st.total_borrowed
        total_repaid += st.total_repaid
        total_interest_accrued += st.total_interest_accrued
        outstanding += st.principal
        total_principal_time_seconds += st.principal_time_seconds

    # overall average APR and APY across reserves (time-weighted by principal*time)
    if total_principal_time_seconds > 0:
        overall_avg_apr = (total_interest_accrued * SECONDS_PER_YEAR) / total_principal_time_seconds
        overall_avg_apy = (1.0 + (overall_avg_apr / SECONDS_PER_YEAR)) ** SECONDS_PER_YEAR - 1.0
    else:
        overall_avg_apr = 0.0
        overall_avg_apy = 0.0

    summary = {
        "per_reserve": summary_per_reserve,
        "totals": {
            "borrowed": total_borrowed,
            "repaid": total_repaid,
            "interest_accrued": total_interest_accrued,
            "outstanding_principal": outstanding,
            "average_apr": overall_avg_apr * 100,
            "average_apy": overall_avg_apy * 100,
        },
    }

    return df, summary


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Hyperlend loan evolution and interest analytics")
    p.add_argument("address_pos", nargs="?", help="User wallet address (checksummed)")
    p.add_argument("--address", default=os.getenv("HYPERLEND_ADDRESS"), help="User wallet address (checksummed)")
    p.add_argument("--chain", default=os.getenv("HYPERLEND_CHAIN", "hyperEvm"), help="Chain (default: hyperEvm)")
    # --token remains available but not required; by default we auto-detect all debt assets from history
    p.add_argument("--token", default=os.getenv("HYPERLEND_TOKEN"), help="Optional debt asset address to filter")
    p.add_argument("--base-url", default=os.getenv("HYPERLEND_BASE_URL", "https://api.hyperlend.finance"))
    p.add_argument("--output-csv", default="loan.csv", help="Path to write per-event evolution CSV (default: loan.csv)")
    p.add_argument("--as-of", default="now", help="Accrue interest up to this unix timestamp in seconds (default: now)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    address = args.address_pos or args.address
    if not address:
        print("Error: address is required (provide positional address, --address, or HYPERLEND_ADDRESS)", file=sys.stderr)
        return 2

    if isinstance(args.as_of, str) and args.as_of.lower() == "now":
        as_of_ts = int(time.time())
    else:
        try:
            as_of_ts = int(args.as_of)
        except Exception:
            print("Error: --as-of must be 'now' or a unix timestamp in seconds", file=sys.stderr)
            return 2

    try:
        df, summary = analyze_loans(
            chain=args.chain,
            address=address,
            base_url=args.base_url,
            token_filter=args.token,
            as_of_ts=as_of_ts,
        )
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print("=== Loan Summary ===")
    print(pd.Series(summary["totals"]))
    print()
    print("=== Per-Reserve Summary ===")
    if summary["per_reserve"]:
        print(pd.DataFrame(summary["per_reserve"]).T)
    else:
        print("(no reserves)")

    out_csv = args.output_csv or "loan.csv"
    if df.empty:

        columns = [
            "reserve","blockNumber","logIndex","timestamp","datetime","event","amount",
            "principal_before","accrued_interest_since_last","principal_after",
            "total_borrowed_so_far","total_repaid_so_far","total_interest_accrued_so_far",
        ]
        pd.DataFrame(columns=columns).to_csv(out_csv, index=False)
        print(f"No loan events found. Wrote empty CSV with headers to {out_csv}")
    else:
        df.to_csv(out_csv, index=False)
        print(f"Wrote per-event evolution to {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
