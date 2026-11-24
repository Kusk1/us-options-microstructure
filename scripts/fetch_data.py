#!/usr/bin/env python3
"""
美股期权微观结构与流动性踩踏监控 — 每日自动数据采集与计算脚本

数据源（100% 免费，零 API Key）：
1. OpenBB Platform (provider="yfinance"):
   - ^VIX, ^VVIX, ^VIX9D, ^VIX3M (VXV), ^SKEW, ^GSPC (SPX), NVDA 日频行情
   - ^SPX 全量期权链（用于 Black-Scholes 计算 SPX GEX / Dealer Agg Gamma / Net Delta Flow / Gamma Flip）
2. CBOE 官方公开 CDN（同时作为 OpenBB 指数的交叉校验与兜底源）:
   - VIXEQ_History.csv, DSPX_History.csv, VIX_History.csv, VVIX_History.csv,
     VIX9D_History.csv, VIX3M_History.csv, SKEW_History.csv
   - 每日市场统计 JSON: https://cdn.cboe.com/data/us/options/market_statistics/daily/{YYYY-MM-DD}_daily_options
     (官方 EQUITY / TOTAL / INDEX PUT/CALL RATIO)

输出：
- docs/data/latest.json  : 最新完整快照、每项来源/观测日/口径/缺口审计、SPX 分行权价 GEX 分布
- docs/data/history.json : 每日历史时间序列（首次运行自动回填近 N 个交易日官方历史，此后每日增量沉淀）
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

# 确保无 HOME 写权限环境（如 CI / 沙箱）OpenBB 也能正常初始化
if not os.environ.get("OPENBB_USER_DATA_DIRECTORY"):
    os.environ.setdefault("OPENBB_AUTO_BUILD", "false")

from openbb import obb  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "docs" / "data"
LATEST_PATH = DATA_DIR / "latest.json"
HISTORY_PATH = DATA_DIR / "history.json"

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
}

CBOE_INDEX_CSV_URLS: dict[str, str] = {
    "vix": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
    "vvix": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv",
    "vix9d": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
    "vix3m": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
    "skew": "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
    "dspx": "https://cdn.cboe.com/api/global/us_indices/daily_prices/DSPX_History.csv",
    "vixeq": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIXEQ_History.csv",
}

OPENBB_INDEX_SYMBOLS: dict[str, str] = {
    "vix": "^VIX",
    "vvix": "^VVIX",
    "vix9d": "^VIX9D",
    "vix3m": "^VIX3M",
    "skew": "^SKEW",
    "spx": "^GSPC",
}

OPENBB_EQUITY_SYMBOLS: dict[str, str] = {
    "nvda": "NVDA",
}

CBOE_DAILY_STATS_URL = (
    "https://cdn.cboe.com/data/us/options/market_statistics/daily/{date_str}_daily_options"
)


def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 1. OpenBB & CBOE 日频时间序列拉取
# ---------------------------------------------------------------------------


def fetch_openbb_series(
    symbol: str,
    is_index: bool,
    start_date: str,
) -> pd.Series | None:
    """使用 OpenBB (yfinance provider) 拉取收盘价序列，索引为 YYYY-MM-DD 字符串。"""
    try:
        if is_index:
            res = obb.index.price.historical(
                symbol=symbol, start_date=start_date, provider="yfinance"
            )
        else:
            res = obb.equity.price.historical(
                symbol=symbol, start_date=start_date, provider="yfinance"
            )
        df = res.to_df()
        if df is None or df.empty or "close" not in df.columns:
            return None
        s = pd.to_numeric(df["close"], errors="coerce").dropna()
        s.index = pd.to_datetime(s.index).strftime("%Y-%m-%d")
        s = s[~s.index.duplicated(keep="last")].sort_index()
        return s
    except Exception as exc:
        log(f"  [WARN] OpenBB yfinance failed for {symbol}: {type(exc).__name__}: {exc}")
        return None


def fetch_cboe_index_csv(name: str, url: str) -> pd.Series | None:
    """从 CBOE 官方公开 CDN 拉取历史日频 CSV。"""
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            return None
        cols_upper = {c.strip().upper(): c for c in df.columns}
        date_col = cols_upper.get("DATE") or df.columns[0]
        # 优先 CLOSE，其次同名列（如 VVIX / SKEW / DSPX / VIXEQ），再其次最后一列
        val_col = (
            cols_upper.get("CLOSE")
            or cols_upper.get(name.upper())
            or df.columns[-1]
        )
        dates = pd.to_datetime(df[date_col], errors="coerce")
        vals = pd.to_numeric(df[val_col], errors="coerce")
        clean = pd.DataFrame({"date": dates, "val": vals}).dropna()
        clean["date_str"] = clean["date"].dt.strftime("%Y-%m-%d")
        s = clean.set_index("date_str")["val"]
        s = s[~s.index.duplicated(keep="last")].sort_index()
        return s
    except Exception as exc:
        log(f"  [WARN] CBOE CSV failed for {name}: {type(exc).__name__}: {exc}")
        return None


def fetch_cboe_daily_pc(date_str: str) -> dict[str, float] | None:
    """拉取单日 CBOE 官方市场统计 JSON，提取 Equity / Total / Index Put/Call Ratio。"""
    url = CBOE_DAILY_STATS_URL.format(date_str=date_str)
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        payload = resp.json()
        ratios = payload.get("ratios", [])
        mapping: dict[str, float] = {}
        for item in ratios:
            rname = str(item.get("name", "")).strip().upper()
            raw_val = item.get("value")
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue
            if rname == "EQUITY PUT/CALL RATIO":
                mapping["pcEquity"] = round(val, 4)
            elif rname == "TOTAL PUT/CALL RATIO":
                mapping["pcTotal"] = round(val, 4)
            elif rname == "INDEX PUT/CALL RATIO":
                mapping["pcIndex"] = round(val, 4)
        if len(mapping) == 3:
            return mapping
        return mapping if mapping else None
    except Exception:
        return None


def fetch_cboe_pc_batch(dates: list[str], max_workers: int = 8) -> dict[str, dict[str, float]]:
    """并发拉取多日 CBOE 官方 Put/Call Ratio。"""
    results: dict[str, dict[str, float]] = {}
    if not dates:
        return results
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(fetch_cboe_daily_pc, d): d for d in dates}
        for fut in as_completed(future_map):
            d = future_map[fut]
            try:
                res = fut.result()
                if res:
                    results[d] = res
            except Exception:
                pass
    return results


# ---------------------------------------------------------------------------
# 2. OpenBB ^SPX 期权链 -> Black-Scholes 做市商敞口计算
# ---------------------------------------------------------------------------


def _norm_cdf_vec(x: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + np.vectorize(math.erf)(x / math.sqrt(2.0)))


def compute_spx_dealer_exposure(
    risk_free_rate: float = 0.043,
    dividend_yield: float = 0.013,
) -> dict[str, Any]:
    """
    通过 OpenBB 拉取 ^SPX 全量期权链，按标准做市商假设计算：
    - spxGex       : SPX GEX 敞口 ($B / 标普波动 1%)
    - dealerGamma  : SPX Dealer Agg Gamma ($B / 标普波动 10 点，约等效于 0.14% 变动)
    - netDelta     : 当日成交量加权净 Delta 资金流 ($B 名义金额)
    - gammaFlip    : Net GEX 穿越零轴的理论点位
    - strikeProfile: 现货附近 ±8% 分行权价 Call/Put/Net GEX 分布（用于前端画墙图）

    说明：
    - 免费期权链不含客户主动买卖方向，沿用 SqueezeMetrics / SpotGamma 通用做市商库存假设：
      Call OI 记为做市商正 Gamma (+1)，Put OI 记为做市商负 Gamma (-1)。
    """
    log("Fetching ^SPX full options chain via OpenBB (yfinance)...")
    res = obb.derivatives.options.chains(symbol="^SPX", provider="yfinance")
    df = res.to_df()
    if df is None or df.empty:
        raise RuntimeError("Empty ^SPX options chain returned by OpenBB")

    spot_series = pd.to_numeric(df["underlying_price"], errors="coerce").dropna()
    if spot_series.empty:
        raise RuntimeError("Missing underlying_price in ^SPX options chain")
    spot = float(spot_series.iloc[0])

    # 推断期权链最新成交日期
    chain_obs_date: str | None = None
    if "last_trade_time" in df.columns:
        lt = pd.to_datetime(df["last_trade_time"], errors="coerce", utc=True).dropna()
        if not lt.empty:
            # 转美东大致日期（UTC-5）
            chain_obs_date = (lt.max() - timedelta(hours=5)).strftime("%Y-%m-%d")

    sub = df.copy()
    sub["open_interest"] = pd.to_numeric(sub["open_interest"], errors="coerce").fillna(0.0)
    sub["volume"] = pd.to_numeric(sub["volume"], errors="coerce").fillna(0.0)
    sub["iv"] = pd.to_numeric(sub["implied_volatility"], errors="coerce").fillna(0.0)
    sub["strike"] = pd.to_numeric(sub["strike"], errors="coerce")
    sub["dte"] = pd.to_numeric(sub["dte"], errors="coerce").fillna(0.0)

    # 过滤掉明显异常的深虚值/失效 IV 报价
    valid = sub[
        (sub["iv"] > 0.01)
        & (sub["iv"] < 4.0)
        & (sub["strike"] > spot * 0.45)
        & (sub["strike"] < spot * 1.55)
        & (sub["dte"] >= 0)
        & (sub["dte"] <= 400)
        & ((sub["open_interest"] > 0) | (sub["volume"] > 0))
    ].copy()

    if valid.empty:
        raise RuntimeError("No valid ^SPX option contracts after filtering")

    T = np.maximum(valid["dte"].values, 0.5) / 365.0
    S = spot
    K = valid["strike"].values
    sigma = valid["iv"].values
    sqrt_T = np.sqrt(T)

    d1 = (np.log(S / K) + (risk_free_rate - dividend_yield + 0.5 * sigma**2) * T) / (
        sigma * sqrt_T
    )
    pdf_d1 = np.exp(-0.5 * d1**2) / math.sqrt(2.0 * math.pi)
    cdf_d1 = _norm_cdf_vec(d1)

    discount_q = np.exp(-dividend_yield * T)
    gamma = discount_q * pdf_d1 / (S * sigma * sqrt_T)
    is_call = (valid["option_type"].astype(str).str.lower() == "call").values
    delta = np.where(is_call, discount_q * cdf_d1, discount_q * (cdf_d1 - 1.0))

    sign = np.where(is_call, 1.0, -1.0)
    oi = valid["open_interest"].values
    vol = valid["volume"].values

    # 1. SPX GEX ($B per 1% move)
    contract_gex_1pct = sign * oi * gamma * 100.0 * (S**2) * 0.01 / 1e9
    spx_gex = float(np.sum(contract_gex_1pct))
    call_gex_1pct = float(np.sum(contract_gex_1pct[is_call]))
    put_gex_1pct = float(np.sum(contract_gex_1pct[~is_call]))

    # 2. SPX Dealer Agg Gamma ($B per 10-point move)
    dealer_gamma_10pt = float(np.sum(sign * oi * gamma * 100.0 * S * 10.0) / 1e9)
    gex_per_1pt_m = float(np.sum(sign * oi * gamma * 100.0 * S) / 1e6)

    # 3. Volume-weighted Net Delta Flow ($B notional)
    # Call delta > 0, Put delta < 0；负值代表当日看跌成交主导、做市商偏机械卖出对冲
    contract_delta_flow_b = vol * delta * 100.0 * S / 1e9
    net_delta_flow_b = float(np.sum(contract_delta_flow_b))

    # 4. Gamma Flip 理论零轴点位（在现价 ±15% 网格上搜索符号翻转点）
    s_grid = np.linspace(S * 0.85, S * 1.15, 61)
    gex_curve: list[tuple[float, float]] = []
    for s_t in s_grid:
        d1_t = (np.log(s_t / K) + (risk_free_rate - dividend_yield + 0.5 * sigma**2) * T) / (
            sigma * sqrt_T
        )
        pdf_t = np.exp(-0.5 * d1_t**2) / math.sqrt(2.0 * math.pi)
        g_t = discount_q * pdf_t / (s_t * sigma * sqrt_T)
        gex_t = float(np.sum(sign * oi * g_t * 100.0 * (s_t**2) * 0.01) / 1e9)
        gex_curve.append((float(s_t), gex_t))

    gamma_flip: float | None = None
    best_dist = float("inf")
    for i in range(len(gex_curve) - 1):
        s0, g0 = gex_curve[i]
        s1, g1 = gex_curve[i + 1]
        if (g0 <= 0.0 <= g1) or (g0 >= 0.0 >= g1):
            frac = abs(g0) / (abs(g0) + abs(g1) + 1e-12)
            cand = s0 + frac * (s1 - s0)
            if abs(cand - S) < best_dist:
                best_dist = abs(cand - S)
                gamma_flip = round(cand, 1)

    # 5. 分行权价 Call/Put/Net GEX 分布（按 25 点分桶，覆盖现价 ±7%），同时识别 Call Wall / Put Wall
    valid["gex_1pct"] = contract_gex_1pct
    valid["call_gex"] = np.where(is_call, contract_gex_1pct, 0.0)
    valid["put_gex"] = np.where(~is_call, contract_gex_1pct, 0.0)
    valid["bucket"] = (np.round(valid["strike"] / 25.0) * 25.0).astype(int)

    near = valid[(valid["strike"] >= S * 0.93) & (valid["strike"] <= S * 1.07)]
    grouped = (
        near.groupby("bucket")[["call_gex", "put_gex", "gex_1pct"]]
        .sum()
        .reset_index()
        .sort_values("bucket")
    )

    above_spot = grouped[grouped["bucket"] >= S]
    below_spot = grouped[grouped["bucket"] <= S]
    call_src = above_spot if not above_spot.empty else grouped
    put_src = below_spot if not below_spot.empty else grouped

    call_wall = (
        int(call_src.loc[call_src["call_gex"].idxmax(), "bucket"])
        if not call_src.empty
        else None
    )
    put_wall = (
        int(put_src.loc[put_src["put_gex"].idxmin(), "bucket"])
        if not put_src.empty
        else None
    )

    strike_profile = [
        {
            "strike": int(row["bucket"]),
            "callGex": round(float(row["call_gex"]), 2),
            "putGex": round(float(row["put_gex"]), 2),
            "netGex": round(float(row["gex_1pct"]), 2),
        }
        for _, row in grouped.iterrows()
    ]

    log(
        f"  SPX chain OK: {len(valid)}/{len(df)} contracts, spot={spot:.2f}, "
        f"GEX={spx_gex:+.2f}$B, DealerGamma={dealer_gamma_10pt:+.2f}$B, "
        f"NetDelta={net_delta_flow_b:+.2f}$B, GammaFlip={gamma_flip}"
    )

    return {
        "spot": round(spot, 2),
        "obsDate": chain_obs_date,
        "totalContracts": int(len(df)),
        "validContracts": int(len(valid)),
        "spxGex": round(spx_gex, 2),
        "callGex": round(call_gex_1pct, 2),
        "putGex": round(put_gex_1pct, 2),
        "dealerGamma": round(dealer_gamma_10pt, 2),
        "gexPer1PtM": round(gex_per_1pt_m, 1),
        "netDelta": round(net_delta_flow_b, 2),
        "gammaFlip": gamma_flip,
        "callWall": call_wall,
        "putWall": put_wall,
        "strikeProfile": strike_profile,
    }


# ---------------------------------------------------------------------------
# 3. 综合压力指数与阶段自动判定（与前端 JS 保持完全一致口径）
# ---------------------------------------------------------------------------


def compute_stress_index(v: dict[str, float | None]) -> tuple[int | None, bool]:
    """
    计算 0~100 综合压力指数。
    若历史回填行缺少做市商敞口字段，则按其余有效维度满分等比归一化，并标记 is_partial=True。
    """
    components: list[tuple[float, float]] = []

    if v.get("vix") is not None:
        components.append((clamp(((v["vix"] - 14.0) / (36.0 - 14.0)) * 15.0, 0.0, 15.0), 15.0))
    if v.get("vvix") is not None:
        components.append((clamp(((v["vvix"] - 85.0) / (135.0 - 85.0)) * 10.0, 0.0, 10.0), 10.0))
    if v.get("vixVxv") is not None:
        components.append((clamp(((v["vixVxv"] - 0.88) / (1.12 - 0.88)) * 9.0, 0.0, 9.0), 9.0))
    if v.get("vix9dVix") is not None:
        components.append((clamp(((v["vix9dVix"] - 0.88) / (1.12 - 0.88)) * 6.0, 0.0, 6.0), 6.0))
    if v.get("spxGex") is not None:
        components.append((clamp(((20.0 - v["spxGex"]) / 140.0) * 12.0, 0.0, 12.0), 12.0))
    if v.get("dealerGamma") is not None:
        components.append((clamp(((1.0 - v["dealerGamma"]) / 10.0) * 10.0, 0.0, 10.0), 10.0))
    if v.get("netDelta") is not None:
        # 单位 $B，典型范围 -60B ~ +60B，零轴附近平稳，-40B 以下高压
        components.append((clamp(((10.0 - v["netDelta"]) / 55.0) * 8.0, 0.0, 8.0), 8.0))
    if v.get("vixeqVix") is not None:
        components.append((clamp(((2.50 - v["vixeqVix"]) / (2.50 - 1.35)) * 15.0, 0.0, 15.0), 15.0))
    if v.get("pcEquity") is not None:
        components.append((clamp(((v["pcEquity"] - 0.50) / (0.90 - 0.50)) * 6.0, 0.0, 6.0), 6.0))
    if v.get("pcTotal") is not None:
        components.append((clamp(((v["pcTotal"] - 0.80) / (1.35 - 0.80)) * 5.0, 0.0, 5.0), 5.0))
    if v.get("pcIndex") is not None:
        components.append((clamp(((v["pcIndex"] - 1.00) / (1.40 - 1.00)) * 4.0, 0.0, 4.0), 4.0))

    max_possible = sum(m for _, m in components)
    if max_possible < 40.0:
        return None, True
    raw_sum = sum(s for s, _ in components)
    is_partial = max_possible < 99.9
    score = round(clamp((raw_sum / max_possible) * 100.0, 0.0, 100.0))
    return int(score), is_partial


def detect_regime(v: dict[str, float | None]) -> dict[str, Any]:
    vix = v.get("vix")
    vvix = v.get("vvix")
    vix_vxv = v.get("vixVxv")
    skew = v.get("skew")
    pc_eq = v.get("pcEquity")
    pc_idx = v.get("pcIndex")
    spx_gex = v.get("spxGex")
    dealer_g = v.get("dealerGamma")
    net_delta = v.get("netDelta")
    vixeq_vix = v.get("vixeqVix")

    macro_defense = bool(
        (pc_idx is not None and pc_eq is not None and pc_idx >= 1.25 and pc_eq >= 0.80)
        or (dealer_g is not None and dealer_g <= -8.6)
    )

    if (
        vix is not None
        and vix >= 26.0
        and ((vix_vxv is not None and vix_vxv >= 1.00) or (vvix is not None and vvix >= 110.0))
    ) or (
        dealer_g is not None
        and spx_gex is not None
        and dealer_g <= -4.5
        and spx_gex <= -50.0
    ):
        return {
            "stage": 3,
            "title": (
                "阶段三升级：Vanna 踩踏与宏观全面防御"
                if macro_defense
                else "阶段三：负反馈翻转与 Vanna 踩踏加速期"
            ),
            "color": "red",
            "macroDefense": macro_defense,
        }

    if (
        (dealer_g is None or dealer_g >= 1.5)
        and (spx_gex is None or spx_gex >= 25.0)
        and vix is not None
        and vix < 18.0
        and (skew is None or skew < 158.0)
    ):
        return {
            "stage": 1,
            "title": "阶段一：缩圈与离散稳态期",
            "color": "green",
            "macroDefense": False,
        }

    if (
        vix_vxv is not None
        and vix_vxv < 1.00
        and vvix is not None
        and vvix < 110.0
        and (skew is None or skew < 155.0)
        and (net_delta is None or net_delta >= 0.0)
        and (spx_gex is None or spx_gex < 25.0)
        and (vixeq_vix is None or vixeq_vix < 2.0)
    ):
        return {
            "stage": 4,
            "title": "阶段四：极端出清与反向 Vanna 修复期",
            "color": "blue",
            "macroDefense": False,
        }

    return {
        "stage": 2,
        "title": "阶段二：拥挤见顶与先遣撤退期",
        "color": "amber",
        "macroDefense": False,
    }


# ---------------------------------------------------------------------------
# 4. 主编排流程：拉取、对齐、合并历史并落盘
# ---------------------------------------------------------------------------


def load_existing_history() -> dict[str, dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return {}
    try:
        raw = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        records = raw.get("records", []) if isinstance(raw, dict) else []
        return {r["date"]: r for r in records if isinstance(r, dict) and "date" in r}
    except Exception as exc:
        log(f"[WARN] Could not load existing history.json: {exc}")
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch US options microstructure data via OpenBB + CBOE")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=90,
        help="Number of recent trading days to backfill when history is sparse (default: 90)",
    )
    parser.add_argument(
        "--skip-options-chain",
        action="store_true",
        help="Skip ^SPX options chain calculation (fast debug mode)",
    )
    args = parser.parse_args()

    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = (date.today() - timedelta(days=max(args.backfill_days * 2, 200))).isoformat()

    log(f"Starting daily fetch (start_date={start_date}, backfill_days={args.backfill_days})")

    # 1. 拉取 OpenBB 序列（主源）与 CBOE 官方 CSV（VIXEQ / DSPX 主源 + 其他波动率指数交叉/兜底）
    series_map: dict[str, pd.Series] = {}
    source_meta: dict[str, dict[str, str]] = {}

    log("Fetching OpenBB (yfinance) index & equity series...")
    for key, sym in OPENBB_INDEX_SYMBOLS.items():
        s = fetch_openbb_series(sym, is_index=True, start_date=start_date)
        if s is not None and not s.empty:
            series_map[key] = s
            source_meta[key] = {"source": f"OpenBB ({sym} via yfinance)", "kind": "official"}
            log(f"  OK OpenBB {key:6s} ({sym:6s}): {len(s)} rows, last={s.index[-1]} ({s.iloc[-1]:.2f})")

    for key, sym in OPENBB_EQUITY_SYMBOLS.items():
        s = fetch_openbb_series(sym, is_index=False, start_date=start_date)
        if s is not None and not s.empty:
            series_map[key] = s
            source_meta[key] = {"source": f"OpenBB ({sym} via yfinance)", "kind": "official"}
            log(f"  OK OpenBB {key:6s} ({sym:6s}): {len(s)} rows, last={s.index[-1]} ({s.iloc[-1]:.2f})")

    log("Fetching CBOE official public CSV indices...")
    cboe_csvs: dict[str, pd.Series] = {}
    for key, url in CBOE_INDEX_CSV_URLS.items():
        s = fetch_cboe_index_csv(key, url)
        if s is not None and not s.empty:
            s = s[s.index >= start_date]
            cboe_csvs[key] = s
            log(f"  OK CBOE   {key:6s}: {len(s)} rows, last={s.index[-1]} ({s.iloc[-1]:.2f})")

    # VIXEQ / DSPX 使用 CBOE 官方 CSV 作为主源；其他波动率指标在 OpenBB 缺失或日期滞后时以 CBOE 补齐
    for key in ["vixeq", "dspx"]:
        if key in cboe_csvs:
            series_map[key] = cboe_csvs[key]
            source_meta[key] = {"source": f"CBOE Official CSV ({key.upper()})", "kind": "official"}

    for key in ["vix", "vvix", "vix9d", "vix3m", "skew"]:
        if key not in series_map and key in cboe_csvs:
            series_map[key] = cboe_csvs[key]
            source_meta[key] = {"source": f"CBOE Official CSV ({key.upper()})", "kind": "official"}
        elif key in series_map and key in cboe_csvs:
            # 用 CBOE 官方日终数据补齐 OpenBB 缺失的历史日期，但不覆盖 OpenBB 已有日期
            combined = series_map[key].combine_first(cboe_csvs[key]).sort_index()
            series_map[key] = combined

    # 2. 确定权威最新完整交易日（以标普 SPX / NVDA / CBOE 波动率一致的最新完整收盘日为准，避免盘前未收盘残缺行）
    anchor_dates: list[str] = []
    for k in ["spx", "nvda", "vvix", "vix3m", "dspx", "vixeq"]:
        if k in series_map and not series_map[k].empty:
            anchor_dates.append(series_map[k].index[-1])
    if not anchor_dates:
        log("[ERROR] Unable to determine any trading date from OpenBB or CBOE!")
        return 1

    # 取出现最多的最新收盘日（通常是美股上一个完整收盘交易日）
    as_of_date = pd.Series(anchor_dates).mode().iloc[0]
    log(f"Canonical latest completed US trading date (as_of_date): {as_of_date}")

    # 确定历史交易日列表（取最近 N 个交易日，截至 as_of_date）
    ref_series = series_map.get("spx") if "spx" in series_map else series_map["vix"]
    all_trading_dates = [d for d in ref_series.index.tolist() if d <= as_of_date]
    target_dates = all_trading_dates[-args.backfill_days :]

    # 3. 载入既有历史并拉取缺失日期的 CBOE 官方 Put/Call Ratio
    existing_history = load_existing_history()
    pc_dates_needed = [
        d
        for d in target_dates
        if (
            d not in existing_history
            or existing_history[d].get("pcEquity") is None
            or existing_history[d].get("pcTotal") is None
            or existing_history[d].get("pcIndex") is None
            or d == as_of_date
        )
    ]
    log(f"Fetching CBOE official daily Put/Call JSON for {len(pc_dates_needed)} date(s)...")
    pc_fetched = fetch_cboe_pc_batch(pc_dates_needed, max_workers=8)
    log(f"  Received CBOE Put/Call data for {len(pc_fetched)}/{len(pc_dates_needed)} date(s)")

    # 4. 计算今日 ^SPX 期权链做市商敞口
    dealer_data: dict[str, Any] | None = None
    dealer_error: str | None = None
    if not args.skip_options_chain:
        try:
            dealer_data = compute_spx_dealer_exposure()
        except Exception as exc:
            dealer_error = f"{type(exc).__name__}: {exc}"
            log(f"[WARN] ^SPX options chain calculation failed: {dealer_error}")

    # 5. 构建/更新每日历史记录（保留已存储的真实历史 GEX/DealerGamma/NetDelta，不覆盖为空）
    def val_on_or_before(key: str, target_d: str, max_lag_days: int = 5) -> tuple[float | None, str | None]:
        s = series_map.get(key)
        if s is None or s.empty:
            return None, None
        sub = s[s.index <= target_d]
        if sub.empty:
            return None, None
        obs_d = str(sub.index[-1])
        lag = (date.fromisoformat(target_d) - date.fromisoformat(obs_d)).days
        if lag > max_lag_days:
            return None, obs_d
        return float(sub.iloc[-1]), obs_d

    updated_history: dict[str, dict[str, Any]] = dict(existing_history)

    for d in target_dates:
        prev = updated_history.get(d, {})
        vix_val, _ = val_on_or_before("vix", d)
        vvix_val, _ = val_on_or_before("vvix", d)
        vix9d_val, _ = val_on_or_before("vix9d", d)
        vix3m_val, _ = val_on_or_before("vix3m", d)
        skew_val, _ = val_on_or_before("skew", d)
        dspx_val, _ = val_on_or_before("dspx", d)
        vixeq_val, _ = val_on_or_before("vixeq", d)
        nvda_val, _ = val_on_or_before("nvda", d)
        spx_val, _ = val_on_or_before("spx", d)

        vixeq_vix = round(vixeq_val / vix_val, 2) if (vixeq_val and vix_val) else None
        vix_vxv = round(vix_val / vix3m_val, 3) if (vix_val and vix3m_val) else None
        vix9d_vix = round(vix9d_val / vix_val, 3) if (vix9d_val and vix_val) else None

        pc_day = pc_fetched.get(d) or {}
        pc_eq = pc_day.get("pcEquity", prev.get("pcEquity"))
        pc_tot = pc_day.get("pcTotal", prev.get("pcTotal"))
        pc_idx = pc_day.get("pcIndex", prev.get("pcIndex"))

        # 做市商敞口：仅对最新交易日写入当日实算值；历史日期保留过去曾实算沉淀下来的值，不伪造
        spx_gex = prev.get("spxGex")
        dealer_gamma = prev.get("dealerGamma")
        net_delta = prev.get("netDelta")
        gamma_flip = prev.get("gammaFlip")

        if d == as_of_date and dealer_data is not None:
            spx_gex = dealer_data["spxGex"]
            dealer_gamma = dealer_data["dealerGamma"]
            net_delta = dealer_data["netDelta"]
            gamma_flip = dealer_data["gammaFlip"]

        row_values: dict[str, float | None] = {
            "vixeqVix": vixeq_vix,
            "vixeq": round(vixeq_val, 2) if vixeq_val is not None else None,
            "dspx": round(dspx_val, 2) if dspx_val is not None else None,
            "nvda": round(nvda_val, 2) if nvda_val is not None else None,
            "spx": round(spx_val, 2) if spx_val is not None else None,
            "vix": round(vix_val, 2) if vix_val is not None else None,
            "vvix": round(vvix_val, 2) if vvix_val is not None else None,
            "vix9d": round(vix9d_val, 2) if vix9d_val is not None else None,
            "vix3m": round(vix3m_val, 2) if vix3m_val is not None else None,
            "vixVxv": vix_vxv,
            "vix9dVix": vix9d_vix,
            "skew": round(skew_val, 2) if skew_val is not None else None,
            "pcEquity": round(pc_eq, 2) if pc_eq is not None else None,
            "pcTotal": round(pc_tot, 2) if pc_tot is not None else None,
            "pcIndex": round(pc_idx, 2) if pc_idx is not None else None,
            "spxGex": round(spx_gex, 2) if spx_gex is not None else None,
            "dealerGamma": round(dealer_gamma, 2) if dealer_gamma is not None else None,
            "netDelta": round(net_delta, 2) if net_delta is not None else None,
            "gammaFlip": round(gamma_flip, 1) if gamma_flip is not None else None,
        }

        stress_score, is_partial_stress = compute_stress_index(row_values)
        regime = detect_regime(row_values)

        updated_history[d] = {
            "date": d,
            **row_values,
            "stressScore": stress_score,
            "stressPartial": is_partial_stress,
            "stage": regime["stage"],
        }

    # 按日期排序并保留最近 365 个交易日
    sorted_dates = sorted(updated_history.keys())[-365:]
    history_records = [updated_history[d] for d in sorted_dates]

    # 6. 组装最新一期 latest.json（包含逐指标来源、观测日、单位、口径与缺口审计）
    latest_row = updated_history[as_of_date]
    gaps: list[dict[str, str]] = []

    def build_provenance(
        key: str,
        raw_series_key: str | None,
        unit: str,
        source: str,
        kind: str,
        note: str = "",
        explicit_obs_date: str | None = None,
    ) -> dict[str, Any]:
        val = latest_row.get(key)
        obs_d = explicit_obs_date
        if obs_d is None and raw_series_key:
            _, obs_d = val_on_or_before(raw_series_key, as_of_date)
        status = "ok"
        if val is None:
            status = "missing"
            gaps.append({"key": key, "reason": note or "当日数据源未返回有效值"})
        elif obs_d and obs_d < as_of_date:
            status = "stale"
            gaps.append({"key": key, "reason": f"最新观测日为 {obs_d}，落后基准日 {as_of_date}"})
        return {
            "value": val,
            "unit": unit,
            "obsDate": obs_d or as_of_date,
            "source": source,
            "kind": kind,  # official | derived | model_estimate
            "status": status,
            "note": note,
        }

    # 确定 Put/Call 最新观测日（若基准日当天 CBOE 尚未出表，回看最近 3 个交易日并明确标注 stale）
    pc_obs_date = as_of_date
    if latest_row.get("pcEquity") is None:
        for back_d in reversed(sorted_dates[-5:]):
            cand = updated_history[back_d]
            if cand.get("pcEquity") is not None:
                latest_row = dict(latest_row)
                latest_row["pcEquity"] = cand["pcEquity"]
                latest_row["pcTotal"] = cand["pcTotal"]
                latest_row["pcIndex"] = cand["pcIndex"]
                pc_obs_date = back_d
                break

    # 若今日 ^SPX 期权链失败，尝试沿用最近一次历史实算值并明确标 stale（绝不补 0）
    dealer_obs_date = (dealer_data or {}).get("obsDate") or as_of_date
    if dealer_data is None:
        for back_d in reversed(sorted_dates[-5:]):
            cand = updated_history[back_d]
            if cand.get("spxGex") is not None:
                latest_row = dict(latest_row)
                latest_row["spxGex"] = cand["spxGex"]
                latest_row["dealerGamma"] = cand["dealerGamma"]
                latest_row["netDelta"] = cand["netDelta"]
                latest_row["gammaFlip"] = cand.get("gammaFlip")
                dealer_obs_date = back_d
                break

    provenance = {
        "vixeqVix": build_provenance(
            "vixeqVix",
            "vixeq",
            "x",
            "派生：CBOE VIXEQ ÷ OpenBB ^VIX",
            "derived",
            f"VIXEQ={latest_row.get('vixeq')} / VIX={latest_row.get('vix')}",
        ),
        "dspx": build_provenance(
            "dspx",
            "dspx",
            "点",
            "CBOE Official CSV (DSPX)",
            "official",
            "Cboe S&P 500 30日离散度指数",
        ),
        "nvda": build_provenance(
            "nvda",
            "nvda",
            "USD",
            source_meta.get("nvda", {}).get("source", "OpenBB (NVDA via yfinance)"),
            "official",
            "权重科技龙头风向标收盘价",
        ),
        "vix": build_provenance(
            "vix",
            "vix",
            "点",
            source_meta.get("vix", {}).get("source", "OpenBB (^VIX via yfinance)"),
            "official",
            "30天基准恐慌指数收盘值",
        ),
        "vvix": build_provenance(
            "vvix",
            "vvix",
            "点",
            source_meta.get("vvix", {}).get("source", "OpenBB (^VVIX via yfinance)"),
            "official",
            "VIX 期权隐含波动率（波动率的波动率）",
        ),
        "vixVxv": build_provenance(
            "vixVxv",
            "vix3m",
            "x",
            "派生：OpenBB ^VIX ÷ ^VIX3M (VXV)",
            "derived",
            f"VIX={latest_row.get('vix')} / VIX3M={latest_row.get('vix3m')}",
        ),
        "vix9dVix": build_provenance(
            "vix9dVix",
            "vix9d",
            "x",
            "派生：OpenBB ^VIX9D ÷ ^VIX",
            "derived",
            f"VIX9D={latest_row.get('vix9d')} / VIX={latest_row.get('vix')}",
        ),
        "skew": build_provenance(
            "skew",
            "skew",
            "点",
            source_meta.get("skew", {}).get("source", "OpenBB (^SKEW via yfinance)"),
            "official",
            "CBOE 标普尾部风险偏度指数",
        ),
        "pcEquity": build_provenance(
            "pcEquity",
            None,
            "比值",
            "CBOE Daily Market Statistics JSON",
            "official",
            "官方个股 Put/Call 成交量比",
            explicit_obs_date=pc_obs_date,
        ),
        "pcTotal": build_provenance(
            "pcTotal",
            None,
            "比值",
            "CBOE Daily Market Statistics JSON",
            "official",
            "官方全市场汇总 Put/Call 成交量比",
            explicit_obs_date=pc_obs_date,
        ),
        "pcIndex": build_provenance(
            "pcIndex",
            None,
            "比值",
            "CBOE Daily Market Statistics JSON",
            "official",
            "官方指数 Put/Call 成交量比",
            explicit_obs_date=pc_obs_date,
        ),
        "spxGex": build_provenance(
            "spxGex",
            None,
            "$B / 1%变动",
            "OpenBB ^SPX 期权链 → Black-Scholes 自算",
            "model_estimate",
            dealer_error or "标准做市商假设（Call +OI / Put -OI），非付费订单流口径",
            explicit_obs_date=dealer_obs_date,
        ),
        "dealerGamma": build_provenance(
            "dealerGamma",
            None,
            "$B / 10点变动",
            "OpenBB ^SPX 期权链 → Black-Scholes 自算",
            "model_estimate",
            dealer_error or "每 10 点 SPX 变动的做市商 Gamma 美元敞口估算",
            explicit_obs_date=dealer_obs_date,
        ),
        "netDelta": build_provenance(
            "netDelta",
            None,
            "$B 名义流",
            "OpenBB ^SPX 期权链 → 当日成交量加权 Delta 流",
            "model_estimate",
            dealer_error or "按当日 Call/Put 成交量 × Delta × 100 × Spot 估算",
            explicit_obs_date=dealer_obs_date,
        ),
    }

    final_stress, final_partial = compute_stress_index(latest_row)
    final_regime = detect_regime(latest_row)

    latest_payload = {
        "schemaVersion": "1.0.0",
        "asOfDate": as_of_date,
        "fetchedAtUtc": fetched_at_utc,
        "spxSpot": latest_row.get("spx"),
        "stressScore": final_stress,
        "stressPartial": final_partial,
        "regime": final_regime,
        "values": {
            "vixeqVix": latest_row.get("vixeqVix"),
            "dspx": latest_row.get("dspx"),
            "nvda": latest_row.get("nvda"),
            "vix": latest_row.get("vix"),
            "vvix": latest_row.get("vvix"),
            "vixVxv": latest_row.get("vixVxv"),
            "vix9dVix": latest_row.get("vix9dVix"),
            "skew": latest_row.get("skew"),
            "pcEquity": latest_row.get("pcEquity"),
            "pcTotal": latest_row.get("pcTotal"),
            "pcIndex": latest_row.get("pcIndex"),
            "spxGex": latest_row.get("spxGex"),
            "dealerGamma": latest_row.get("dealerGamma"),
            "netDelta": latest_row.get("netDelta"),
        },
        "aux": {
            "vixeq": latest_row.get("vixeq"),
            "vix9d": latest_row.get("vix9d"),
            "vix3m": latest_row.get("vix3m"),
            "spx": latest_row.get("spx"),
            "gammaFlip": latest_row.get("gammaFlip"),
            "callWall": (dealer_data or {}).get("callWall"),
            "putWall": (dealer_data or {}).get("putWall"),
            "callGex": (dealer_data or {}).get("callGex"),
            "putGex": (dealer_data or {}).get("putGex"),
            "gexPer1PtM": (dealer_data or {}).get("gexPer1PtM"),
            "validContracts": (dealer_data or {}).get("validContracts"),
            "totalContracts": (dealer_data or {}).get("totalContracts"),
        },
        "strikeProfile": (dealer_data or {}).get("strikeProfile", []),
        "provenance": provenance,
        "gaps": gaps,
    }

    history_payload = {
        "schemaVersion": "1.0.0",
        "updatedAtUtc": fetched_at_utc,
        "latestDate": as_of_date,
        "count": len(history_records),
        "records": history_records,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_PATH.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HISTORY_PATH.write_text(json.dumps(history_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"Saved {LATEST_PATH} and {HISTORY_PATH} ({len(history_records)} trading days, {len(gaps)} gap(s)).")
    log(f"Summary -> asOf={as_of_date}, Stress={final_stress}/100, Stage={final_regime['stage']} ({final_regime['title']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
