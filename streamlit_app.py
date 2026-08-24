from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from fx_cache import FxCacheBackgroundUpdater, ensure_fx_cache_files, fx_cache_version
from fx_cache import FX_CACHE_TO_CCY, load_fx_cache_series
import invest_processing as inv
from processing import (
    PreparedData,
    add_category_mapping,
    append_manual_expense,
    cleanup_outdated_account_statement_csvs,
    find_latest_account_statement_csv,
    find_latest_savings_statement_csv,
    load_revolut_csv,
    load_expense_category_map,
    load_manual_expenses,
    load_monthly_limits,
    normalize_revolut_df,
    prepare_data_for_plotting,
    refund_cross_month_summary,
    successful_transaction_mask,
)


def fmt_dkk(x: float) -> str:
    return f"{x:,.0f}"



@st.cache_data(show_spinner=True)
def load_prepared(
    csv_path: str,
    fx_version: float,
    manual_version: float,
    csv_version: float,
    refresh_nonce: int,
) -> PreparedData:
    # Version args exist to invalidate the cache when source files change or user presses refresh.
    # Bump _schema_version whenever PreparedData's shape changes so cached results are rebuilt.
    _schema_version = 2  # added per-category item_count to spend_by_month_category
    _ = (fx_version, manual_version, csv_version, refresh_nonce, _schema_version)
    return prepare_data_for_plotting(csv_path, manual_data_dir="data")


def manual_expenses_version(data_dir: str = "data") -> float:
    try:
        import os

        p = os.path.join(data_dir, "manual_expenses.csv")
        return os.path.getmtime(p) if os.path.exists(p) else 0.0
    except Exception:
        return 0.0


def file_mtime(path: str) -> float:
    try:
        import os

        return os.path.getmtime(path) if os.path.exists(path) else 0.0
    except Exception:
        return 0.0


@st.cache_data(show_spinner=True)
def load_investment_summary(
    account_csv_path: str,
    consolidated_csv_path: str,
    fx_version: float,
    account_version: float,
    consolidated_version: float,
) -> dict[str, object]:
    _ = (fx_version, account_version, consolidated_version)

    # Use DKK-side exchange rows. This exists even when the foreign leg is moved/recorded
    # in the investment statement days later.
    dkk_exchanges = inv.extract_dkk_exchanges_from_account_statement(account_csv_path)
    if not dkk_exchanges.empty:
        dkk_exchanges["to_currency"] = dkk_exchanges["to_currency"].astype(str).str.upper().str.strip()
        dkk_exchanges = dkk_exchanges.loc[dkk_exchanges["to_currency"].isin(["USD", "GBP"])].copy()

    invest_tx = inv.parse_consolidated_investment_statement(consolidated_csv_path)
    interest = invest_tx.loc[invest_tx.get("action").astype(str).str.upper().eq("INTEREST")].copy()
    interest["currency"] = interest.get("currency", "").astype(str).str.upper().str.strip()
    interest["value"] = pd.to_numeric(interest.get("value"), errors="coerce")

    interest_totals = (
        interest.groupby("currency", dropna=False)["value"].sum().sort_index().reset_index()
        if not interest.empty
        else pd.DataFrame(columns=["currency", "value"])
    )

    # Get max transaction date from investment statement
    invest_max_date = None
    if not invest_tx.empty and "tx_datetime" in invest_tx.columns:
        invest_tx["tx_datetime"] = pd.to_datetime(invest_tx["tx_datetime"], errors="coerce")
        invest_max_date = invest_tx["tx_datetime"].max()

    today = pd.Timestamp.today().normalize()

    if dkk_exchanges.empty:
        fx_detail = dkk_exchanges.copy() if isinstance(dkk_exchanges, pd.DataFrame) else pd.DataFrame()
        fx_totals = pd.DataFrame(
            columns=[
                "currency",
                "dkk_exchanged",
                "foreign_bought_est",
                "dkk_value_at_today_fx",
                "dkk_change",
                "pct_change",
            ]
        )
    else:
        fx_detail = dkk_exchanges.copy()
        fx_detail["completed_day"] = pd.to_datetime(
            fx_detail["exchange_completed_date"], errors="coerce"
        ).dt.normalize()
        fx_detail["currency"] = fx_detail["to_currency"].astype(str).str.upper().str.strip()
        fx_detail["dkk_exchanged"] = pd.to_numeric(fx_detail["from_amount"], errors="coerce")

        def rate_on(day: pd.Timestamp, from_ccy: str) -> float | None:
            s = load_fx_cache_series(from_ccy, data_dir="data", to_ccy=FX_CACHE_TO_CCY)
            if s.empty or pd.isna(day):
                return None
            v = s.get(pd.Timestamp(day).normalize())
            try:
                return float(v) if v is not None and not pd.isna(v) else None
            except Exception:
                return None

        # Cache today's rates per currency
        today_rates: dict[str, float | None] = {}
        for c in sorted(set(fx_detail["currency"].dropna().unique().tolist())):
            today_rates[c] = rate_on(today, c)

        rate_at_exchange: list[float | None] = []
        rate_at_today: list[float | None] = []
        for _idx, row in fx_detail.iterrows():
            ccy = str(row.get("currency") or "").upper().strip()
            day = row.get("completed_day")
            r0 = rate_on(pd.Timestamp(day) if day is not None else pd.NaT, ccy)
            r1 = today_rates.get(ccy)
            rate_at_exchange.append(r0)
            rate_at_today.append(r1)

        fx_detail["fx_rate_dkk_per_ccy_at_exchange_day"] = rate_at_exchange
        fx_detail["fx_rate_dkk_per_ccy_today"] = rate_at_today

        # Estimate foreign bought using FX at the exchange day:
        #   foreign_bought_est = dkk_exchanged / (dkk_per_ccy)
        fx_detail["foreign_bought_est"] = fx_detail["dkk_exchanged"] / pd.to_numeric(
            fx_detail["fx_rate_dkk_per_ccy_at_exchange_day"], errors="coerce"
        )
        fx_detail["dkk_value_at_today_fx"] = fx_detail["foreign_bought_est"] * pd.to_numeric(
            fx_detail["fx_rate_dkk_per_ccy_today"], errors="coerce"
        )
        fx_detail["dkk_change"] = fx_detail["dkk_value_at_today_fx"] - fx_detail["dkk_exchanged"]
        fx_detail["pct_change"] = np.where(
            fx_detail["dkk_exchanged"].astype(float) != 0,
            fx_detail["dkk_change"] / fx_detail["dkk_exchanged"],
            np.nan,
        )

        fx_totals = (
            fx_detail.groupby("currency", dropna=False)
            .agg(
                dkk_exchanged=("dkk_exchanged", "sum"),
                foreign_bought_est=("foreign_bought_est", "sum"),
                dkk_value_at_today_fx=("dkk_value_at_today_fx", "sum"),
            )
            .reset_index()
        )
        fx_totals["dkk_change"] = fx_totals["dkk_value_at_today_fx"] - fx_totals["dkk_exchanged"]
        fx_totals["pct_change"] = np.where(
            fx_totals["dkk_exchanged"].astype(float) != 0,
            fx_totals["dkk_change"] / fx_totals["dkk_exchanged"],
            np.nan,
        )

    return {
        "dkk_exchanges": dkk_exchanges,
        "fx_detail": fx_detail,
        "fx_totals": fx_totals,
        "interest_totals": interest_totals,
        "interest_rows": interest,
        "today": today,
        "summary": inv.parse_investment_summary(consolidated_csv_path),
        "invest_max_date": invest_max_date,
    }


@st.cache_data(show_spinner=False)
def category_options() -> list[str]:
    # unique categories from YAML mapping
    compiled = load_expense_category_map()
    cats = sorted({cat for _kw, cat in compiled if cat})
    return cats


def _fx_rate_on_or_before(series: pd.Series, day: pd.Timestamp) -> float | None:
    if series is None or series.empty or pd.isna(day):
        return None
    s = series.copy().sort_index()
    d = pd.Timestamp(day).normalize()
    window = s.loc[s.index <= d]
    if window.empty:
        return None
    v = window.iloc[-1]
    try:
        return float(v) if v is not None and not pd.isna(v) else None
    except Exception:
        return None


def compute_exchange_fx_pnl(account_csv_path: str, data_dir: str = "data") -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    """Compute P/L for DKK->USD/GBP exchanges valued at today's FX."""

    raw = load_revolut_csv(account_csv_path)
    df = normalize_revolut_df(raw)

    today = pd.Timestamp.today().normalize()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), today

    sub_type = df.get("sub_type", pd.Series("", index=df.index, dtype="object")).astype(str)
    desc = df.get("description", pd.Series("", index=df.index, dtype="object")).astype(str)
    ccy = df.get("currency", pd.Series("", index=df.index, dtype="object")).astype(str).str.upper().str.strip()
    amt = pd.to_numeric(df.get("amount_net"), errors="coerce")
    completed = pd.to_datetime(df.get("completed_date"), errors="coerce")
    success = successful_transaction_mask(df)

    ex_mask = (
        sub_type.str.casefold().eq("exchange")
        & desc.str.contains("Exchanged to", case=False, na=False)
        & ccy.eq("DKK")
        & amt.lt(0)
        & completed.notna()
        & success
    )
    ex = df.loc[ex_mask].copy()
    if ex.empty:
        return pd.DataFrame(), pd.DataFrame(), today

    ex["to_currency"] = (
        ex["description"].astype(str).str.extract(r"Exchanged to\s+([A-Za-z]{3})", expand=False).str.upper()
    )
    ex = ex[ex["to_currency"].isin(["USD", "GBP"])].copy()
    if ex.empty:
        return pd.DataFrame(), pd.DataFrame(), today

    ex["completed_day"] = pd.to_datetime(ex["completed_date"], errors="coerce").dt.normalize()
    ex["dkk_out"] = pd.to_numeric(ex["amount_net"], errors="coerce").abs()

    fx_series = {
        "USD": load_fx_cache_series("USD", data_dir=data_dir, to_ccy=FX_CACHE_TO_CCY),
        "GBP": load_fx_cache_series("GBP", data_dir=data_dir, to_ccy=FX_CACHE_TO_CCY),
    }

    ex["fx_at_exchange"] = ex.apply(
        lambda r: _fx_rate_on_or_before(fx_series.get(str(r["to_currency"])), r["completed_day"]),
        axis=1,
    )
    ex["fx_today"] = ex["to_currency"].map(
        lambda c: _fx_rate_on_or_before(fx_series.get(str(c)), today)
    )
    ex["foreign_bought"] = ex["dkk_out"] / pd.to_numeric(ex["fx_at_exchange"], errors="coerce")
    ex["dkk_value_today"] = ex["foreign_bought"] * pd.to_numeric(ex["fx_today"], errors="coerce")
    ex["dkk_pnl"] = ex["dkk_value_today"] - ex["dkk_out"]
    ex["pnl_pct"] = np.where(ex["dkk_out"] != 0, ex["dkk_pnl"] / ex["dkk_out"], np.nan)

    detail = ex[
        [
            "completed_date",
            "description",
            "to_currency",
            "dkk_out",
            "fx_at_exchange",
            "foreign_bought",
            "fx_today",
            "dkk_value_today",
            "dkk_pnl",
            "pnl_pct",
        ]
    ].copy()
    detail = detail.sort_values("completed_date", ascending=True)

    totals = (
        detail.groupby("to_currency", dropna=False)
        .agg(
            dkk_out=("dkk_out", "sum"),
            foreign_bought=("foreign_bought", "sum"),
            dkk_value_today=("dkk_value_today", "sum"),
            dkk_pnl=("dkk_pnl", "sum"),
        )
        .reset_index()
    )
    totals["pnl_pct"] = np.where(totals["dkk_out"] != 0, totals["dkk_pnl"] / totals["dkk_out"], np.nan)

    return detail, totals, today


def _to_float_from_csv(value: object) -> float | None:
    s = str(value or "").strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def compute_savings_interest_summary(savings_csv_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute net interest by matching Interest PAID and Service Fee Charged rows per timestamp/currency."""

    rows: list[dict[str, object]] = []
    active_currency: str | None = None

    with open(savings_csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            first = str(row[0]).strip()
            second = str(row[1]).strip() if len(row) > 1 else ""
            if first == "Date" and second == "Description":
                joined = " ".join(str(x) for x in row)
                if "Value, USD" in joined:
                    active_currency = "USD"
                elif "Value, GBP" in joined:
                    active_currency = "GBP"
                else:
                    active_currency = None
                continue

            if active_currency not in {"USD", "GBP"}:
                continue
            if len(row) < 5:
                continue

            dt = pd.to_datetime(first, errors="coerce")
            description = second
            foreign_value = _to_float_from_csv(row[2])
            dkk_value = _to_float_from_csv(row[3]) if len(row) > 3 else None
            fx_rate = _to_float_from_csv(row[4]) if len(row) > 4 else None
            if pd.isna(dt) or not description or foreign_value is None or dkk_value is None:
                continue

            if description.startswith(f"Interest PAID {active_currency}"):
                entry_type = "interest"
            elif description.startswith(f"Service Fee Charged {active_currency}"):
                entry_type = "fee"
            else:
                continue

            rows.append(
                {
                    "datetime": dt,
                    "currency": active_currency,
                    "entry_type": entry_type,
                    "foreign_value": float(foreign_value),
                    "dkk_value": float(dkk_value),
                    "fx_rate": float(fx_rate) if fx_rate is not None else None,
                }
            )

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    raw = pd.DataFrame(rows)
    detail = (
        raw.groupby(["datetime", "currency"], dropna=False)
        .agg(
            interest_foreign=("foreign_value", lambda s: float(s[raw.loc[s.index, "entry_type"].eq("interest")].sum())),
            fee_foreign=("foreign_value", lambda s: float(s[raw.loc[s.index, "entry_type"].eq("fee")].sum())),
            interest_dkk=("dkk_value", lambda s: float(s[raw.loc[s.index, "entry_type"].eq("interest")].sum())),
            fee_dkk=("dkk_value", lambda s: float(s[raw.loc[s.index, "entry_type"].eq("fee")].sum())),
            fx_rate=("fx_rate", "last"),
        )
        .reset_index()
    )
    detail["net_foreign"] = detail["interest_foreign"] + detail["fee_foreign"]
    detail["net_dkk"] = detail["interest_dkk"] + detail["fee_dkk"]
    detail = detail.sort_values("datetime", ascending=False)

    totals = (
        detail.groupby("currency", dropna=False)
        .agg(
            interest_foreign=("interest_foreign", "sum"),
            fee_foreign=("fee_foreign", "sum"),
            net_foreign=("net_foreign", "sum"),
            net_dkk=("net_dkk", "sum"),
        )
        .reset_index()
        .sort_values("currency")
    )
    return detail, totals


@st.cache_resource
def fx_background_updater() -> FxCacheBackgroundUpdater:
    # One updater per Streamlit session.
    return FxCacheBackgroundUpdater(data_dir="data").start()


def plot_month(spend_by_month_category: pd.DataFrame, totals_by_month: pd.DataFrame, month: str):
    plot_df = spend_by_month_category[spend_by_month_category["month"] == month].copy()
    if plot_df.empty:
        return

    s = plot_df.set_index("category")["spend_dkk"].sort_values(ascending=False)
    if s.empty:
        return

    # Per-category item (transaction) counts, aligned to the sorted categories.
    if "item_count" in plot_df.columns:
        counts = plot_df.set_index("category")["item_count"]
    else:
        counts = pd.Series(dtype="int64")
    y_labels = [f"{cat} ({int(counts.get(cat, 0))})" for cat in s.index.astype(str)]

    month_label = pd.Period(month).strftime("%b-%y")

    exp_total = float(totals_by_month.loc[month, "expense"]) if month in totals_by_month.index else 0.0
    inc_total = float(totals_by_month.loc[month, "income"]) if month in totals_by_month.index else 0.0
    ref_total = float(totals_by_month.loc[month, "refund"]) if month in totals_by_month.index else 0.0

    title = f"{month_label}"

    # Style to match the desired dark dashboard look
    bg = "#0e1117"  # Streamlit dark-ish background
    fg = "#e5e7eb"  # light text
    grid = "#374151"  # subtle grid
    bar = "#621b09"  # red bars

    # Use consistent minimum height for better alignment, but allow growth
    fig_h = max(4.5, 0.40 * len(s))
    fig, ax = plt.subplots(figsize=(5.8, fig_h), dpi=120)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    bars = ax.barh(y_labels, s.values, color=bar)
    ax.invert_yaxis()
    ax.set_title(title, loc="left", fontsize=10.5, color=fg, fontweight="bold", pad=6)
    # Compact category count, aligned with the title on the right.
    ax.text(
        1.0,
        1.0,
        f"{len(s)} cats",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        color=fg,
        alpha=0.6,
        fontsize=8,
    )
    ax.set_xlabel("DKK", color=fg, fontsize=9)
    ax.set_ylabel("")

    # Axes / ticks
    ax.tick_params(axis="x", colors=fg, labelsize=8)
    ax.tick_params(axis="y", colors=fg, labelsize=8)
    for spine in ax.spines.values():
        spine.set_visible(False)

    max_val = float(np.nanmax(s.values)) if len(s.values) else 0.0
    ax.set_xlim(0, max(1.0, max_val * 1.12))
    ax.grid(True, axis="x", color=grid, alpha=0.35, linewidth=0.8)
    ax.set_axisbelow(True)

    total_spend = float(np.nansum(s.values)) if len(s.values) else 0.0

    # Value labels (on/near the end of each bar)
    pad_inside = max_val * 0.02
    pad_outside = max_val * 0.015
    for b in bars:
        w = float(b.get_width())
        y = float(b.get_y() + b.get_height() / 2)
        label = fmt_dkk(w)
        pct = (100.0 * w / total_spend) if total_spend > 0 else 0.0
        pct_label = f"({pct:.0f}%)" if pct >= 1.0 else ""
        if max_val > 0 and w >= max_val * 0.12:
            value_text = ax.text(
                w - pad_inside,
                y,
                label,
                va="center",
                ha="right",
                color="#fde047",  # yellow
                fontsize=8.5,
                fontweight="bold",
            )
            if pct_label:
                ax.annotate(
                    pct_label,
                    xy=value_text.get_position(),
                    xycoords="data",
                    textcoords="offset points",
                    xytext=(4, 0),
                    va="center",
                    ha="left",
                    color=fg,
                    fontsize=7.0,
                    fontweight="normal",
                )
        else:
            value_text = ax.text(
                w + pad_outside,
                y,
                label,
                va="center",
                ha="left",
                color=fg,
                fontsize=8.5,
                fontweight="bold",
            )
            if pct_label:
                # Approximate text width (in points) to place percentage after the value.
                x_offset_pts = 6 * len(label) + 6
                ax.annotate(
                    pct_label,
                    xy=value_text.get_position(),
                    xycoords="data",
                    textcoords="offset points",
                    xytext=(x_offset_pts, 0),
                    va="center",
                    ha="left",
                    color=fg,
                    fontsize=7.0,
                    fontweight="normal",
                )

    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def month_totals(totals_by_month: pd.DataFrame, month: str) -> tuple[float, float, float]:
    exp_total = float(totals_by_month.loc[month, "expense"]) if month in totals_by_month.index else 0.0
    inc_total = float(totals_by_month.loc[month, "income"]) if month in totals_by_month.index else 0.0
    ref_total = float(totals_by_month.loc[month, "refund"]) if month in totals_by_month.index else 0.0
    return exp_total, inc_total, ref_total


def refresh_dashboard_data() -> None:
    """Clear cached data and rerun so updated CSV rows are picked up."""
    st.session_state["_refresh_nonce"] = int(st.session_state.get("_refresh_nonce", 0)) + 1
    st.session_state["_just_refreshed"] = True
    st.cache_data.clear()
    st.rerun()


def render_month_table_header(exp_total: float, inc_total: float, ref_total: float, items: int, has_cross_month: bool = False) -> None:
    # Compact caption-style header (small text) like: 💸 5 DKK |  💰 0 DKK |  ♻️ 0 DKK | 📊 1 | ▲ +2
    cross_month_text = " | <span title='Refunds from previous month(s) applied to this month'>🔄</span>" if has_cross_month else ""
    net = inc_total - exp_total
    if net >= 0:
        net_html = f'<span style="color:#21c354;">▲ +{fmt_dkk(net)}</span>'
    else:
        net_html = f'<span style="color:#ff4b4b;">▼ {fmt_dkk(net)}</span>'
    st.markdown(
        "<small>"
        f"💸 <b>{fmt_dkk(exp_total)}</b> DKK | "
        f"💰 {fmt_dkk(inc_total)} DKK | "
        f"♻️ {fmt_dkk(ref_total)} DKK | "
        f"📊 {items} | "
        f"{net_html}{cross_month_text}"
        "</small>",
        unsafe_allow_html=True,
    )


def expenses_table_for_month(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Expense rows for the given month (default-sorted by highest spend)."""
    if df.empty:
        return df
    required = {"completed_date", "type", "description", "amount_dkk", "category"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    tmp = df.copy()
    tmp["completed_date"] = pd.to_datetime(tmp["completed_date"], errors="coerce")
    tmp["amount_dkk"] = pd.to_numeric(tmp["amount_dkk"], errors="coerce")
    tmp = tmp[tmp["type"].astype(str).str.casefold().eq("expense")].copy()
    tmp = tmp[tmp["completed_date"].notna() & tmp["amount_dkk"].notna()].copy()
    tmp["month"] = tmp["completed_date"].dt.to_period("M").astype(str)
    tmp = tmp[tmp["month"] == month].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["datetime", "description", "amount_dkk", "category"])

    tmp["spend_dkk"] = tmp["amount_dkk"].abs()
    tmp = tmp.sort_values(["spend_dkk", "completed_date"], ascending=[False, True])
    out = tmp[["completed_date", "description", "spend_dkk", "category"]].copy()
    out = out.rename(columns={"completed_date": "datetime", "spend_dkk": "amount_dkk"})
    return out


def render_other_expenses_editor(other_df: pd.DataFrame) -> None:
    """Editable table for uncategorized expenses with category assignment."""
    if other_df.empty:
        st.write("No expense rows categorized as 'Other'.")
        return

    other_df = other_df.reset_index().rename(columns={"index": "row"})

    for c in ["completed_date", "started_date"]:
        if c in other_df.columns:
            other_df[c] = pd.to_datetime(other_df[c], errors="coerce")

    for c in ["amount", "fee", "amount_net", "conversion_rate", "amount_dkk", "balance"]:
        if c in other_df.columns:
            other_df[c] = pd.to_numeric(other_df[c], errors="coerce")

    spend_sort = (
        pd.to_numeric(other_df.get("amount_dkk"), errors="coerce").abs()
        if "amount_dkk" in other_df.columns
        else pd.Series([pd.NA] * len(other_df), index=other_df.index)
    )
    fallback = (
        pd.to_numeric(other_df.get("amount_net"), errors="coerce").abs()
        if "amount_net" in other_df.columns
        else pd.Series([pd.NA] * len(other_df), index=other_df.index)
    )
    other_df["spend_sort"] = spend_sort.fillna(fallback)
    other_df = other_df.sort_values(["spend_sort", "completed_date"], ascending=[False, True])

    cols = [
        "row",
        "completed_date",
        "started_date",
        "sub_type",
        "description",
        "currency",
        "amount",
        "fee",
        "amount_net",
        "conversion_rate",
        "amount_dkk",
        "balance",
    ]
    cols = [c for c in cols if c in other_df.columns]

    view = other_df[cols].copy()
    categories = category_options()
    view["assign_category"] = ""

    st.caption("Assign a category and click Apply Category Changes to save into expense_categories.yml")
    edited = st.data_editor(
        view,
        use_container_width=True,
        height=260,
        hide_index=True,
        key="other_expenses_editor",
        column_config={
            "assign_category": st.column_config.SelectboxColumn(
                "Assign Category",
                options=categories,
                help="Choose a category to map this description to",
            ),
        },
        disabled=[c for c in view.columns if c != "assign_category"],
    )

    changes: dict[str, str] = {}
    for _, r in edited.iterrows():
        chosen = str(r.get("assign_category", "")).strip()
        description = str(r.get("description", "")).strip()
        if chosen and description:
            changes[description] = chosen

    st.caption("Preview of mappings to be saved")
    if changes:
        preview_df = pd.DataFrame(
            [{"description": description, "category": category} for description, category in changes.items()]
        ).sort_values(["category", "description"], ascending=[True, True])
        st.dataframe(preview_df, use_container_width=True, hide_index=True, height=180)
    else:
        st.info("No pending category changes selected yet.")

    if st.button(
        "Apply Category Changes",
        type="primary",
        key="apply_category_changes",
        disabled=not bool(changes),
    ):

        for description, chosen in changes.items():
            add_category_mapping(description, chosen)

        st.success(f"Saved {len(changes)} mapping(s) to expense_categories.yml")
        st.cache_data.clear()
        st.rerun()


def plot_current_month_budget_progress(df: pd.DataFrame) -> None:
    """Plot allowed cumulative spend vs actual cumulative spend for the current month."""

    if df.empty:
        return

    required = {"completed_date", "type", "amount_dkk"}
    if not required.issubset(set(df.columns)):
        return

    limits = load_monthly_limits()
    today = pd.Timestamp.today().normalize()
    period = today.to_period("M")
    month_num = int(period.month)
    month_limit = float(limits.get(month_num, 0.0) or 0.0)
    if month_limit <= 0:
        st.caption(
            f"No monthly limit found for {period.strftime('%B')} in expense_categories.yml (or it is 0)."
        )
        return

    month_start = period.start_time.normalize()
    month_end = period.end_time.normalize()
    days = pd.date_range(month_start, month_end, freq="D")
    if len(days) == 0:
        return

    tmp = df.copy()
    tmp["completed_date"] = pd.to_datetime(tmp["completed_date"], errors="coerce")
    tmp["amount_dkk"] = pd.to_numeric(tmp["amount_dkk"], errors="coerce")

    tmp = tmp[tmp["type"].astype(str).str.casefold().eq("expense")].copy()
    tmp = tmp[tmp["completed_date"].notna() & tmp["amount_dkk"].notna()].copy()
    if tmp.empty:
        return

    tmp["day"] = tmp["completed_date"].dt.normalize()
    tmp = tmp[(tmp["day"] >= month_start) & (tmp["day"] <= month_end)].copy()
    if tmp.empty:
        return

    daily_spend = tmp.groupby("day")["amount_dkk"].apply(lambda s: float(s.abs().sum()))
    actual_cum = daily_spend.reindex(days, fill_value=0.0).cumsum()
    # Do not plot into the future
    actual_cum = actual_cum.where(days <= min(today, month_end), np.nan)

    per_day = month_limit / float(len(days))
    allowed_cum = pd.Series(per_day * (np.arange(len(days)) + 1), index=days, dtype="float")

    bg = "#0e1117"
    fg = "#e5e7eb"
    grid = "#374151"

    fig, ax = plt.subplots(figsize=(12.0, 2.8), dpi=120)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    ax.plot(days, allowed_cum.values, color="#f59e0b", linestyle=(0, (4, 4)), linewidth=2.0)

    # Plot actual cumulative spend with conditional coloring:
    # blue when under budget line, red only for the part above.
    from matplotlib.collections import LineCollection
    import matplotlib.dates as mdates

    x = mdates.date2num(pd.to_datetime(days).to_pydatetime())
    y = np.asarray(actual_cum.values, dtype="float")
    a = np.asarray(allowed_cum.values, dtype="float")

    segments: list[np.ndarray] = []
    colors: list[str] = []

    def add_segment(x0: float, y0: float, x1: float, y1: float, above: bool) -> None:
        segments.append(np.array([[x0, y0], [x1, y1]], dtype=float))
        colors.append("#ef4444" if above else "#60a5fa")

    for i in range(len(x) - 1):
        x0, x1 = float(x[i]), float(x[i + 1])
        y0, y1 = float(y[i]), float(y[i + 1])
        a0, a1 = float(a[i]), float(a[i + 1])

        if not np.isfinite(y0) or not np.isfinite(y1):
            continue

        d0 = y0 - a0
        d1 = y1 - a1
        above0 = d0 > 0
        above1 = d1 > 0

        if above0 == above1:
            add_segment(x0, y0, x1, y1, above=above0)
            continue

        # Split at the crossing point where actual == allowed.
        denom = (y1 - y0) - (a1 - a0)
        if denom == 0:
            # Parallel; fall back to coloring by the end point.
            add_segment(x0, y0, x1, y1, above=above1)
            continue

        t = (a0 - y0) / denom
        t = float(np.clip(t, 0.0, 1.0))
        xi = x0 + t * (x1 - x0)
        yi = y0 + t * (y1 - y0)

        add_segment(x0, y0, xi, yi, above=above0)
        add_segment(xi, yi, x1, y1, above=above1)

    if segments:
        lc = LineCollection(
            segments,
            colors=colors,
            linewidths=2.0,
            linestyles=(0, (1, 2)),
        )
        ax.add_collection(lc)

    max_date = pd.to_datetime(df['completed_date'], errors='coerce').max()
    title = f"Cumulative spending (DKK) - {max_date.strftime('%B %Y')}" if pd.notna(max_date) else "Cumulative spending (DKK)"
    ax.set_title(title, color=fg, fontsize=11, fontweight="bold", pad=8)
    
    # Set y-axis with steps of 3000
    from matplotlib.ticker import MultipleLocator
    ax.yaxis.set_major_locator(MultipleLocator(3000))
    
    # Set x-axis with steps of 1 day
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    
    ax.tick_params(axis="x", colors=fg, labelsize=7, rotation=45)
    ax.tick_params(axis="y", colors=fg, labelsize=7)
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Grid lines: horizontal (y-axis) and vertical (x-axis)
    ax.grid(True, axis="y", color=grid, alpha=0.35, linewidth=0.8)
    ax.grid(True, axis="x", color=grid, alpha=0.15, linewidth=0.4)
    ax.set_axisbelow(True)

    # Keep y-axis starting at 0 for readability
    max_y = float(np.nanmax([allowed_cum.max(), actual_cum.max()])) if len(days) else 0.0
    ax.set_ylim(0, max(1.0, max_y * 1.08))

    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def main():
    st.set_page_config(page_title="Revolut expenses", layout="wide")

    st.title("Revolut statement")

    if st.session_state.pop("_just_refreshed", False):
        st.toast("Dashboard data reloaded from disk")

    numbers_docs_dir = "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents"

    try:
        csv_path = find_latest_account_statement_csv(numbers_docs_dir)
    except Exception as e:
        st.error(str(e))
        return

    # Keep workspace tidy: delete older account-statement CSVs.
    cleanup_outdated_account_statement_csvs(numbers_docs_dir, keep_path=csv_path, prefix="account-statement")

    savings_csv_path: str | None = None
    savings_lookup_error: str | None = None
    try:
        savings_csv_path = find_latest_savings_statement_csv(numbers_docs_dir)
        cleanup_outdated_account_statement_csvs(
            numbers_docs_dir,
            keep_path=savings_csv_path,
            prefix="savings-statement",
        )
    except Exception as e:
        savings_lookup_error = str(e)

    st.caption(f"CSV: {csv_path}")

    # FX cache: first run will download and build local CSVs (USD/EUR/GBP->DKK) which can take a bit.
    with st.spinner("Preparing FX cache (first run may take a bit)…"):
        ensure_fx_cache_files(data_dir="data")

    fx_version = fx_cache_version(data_dir="data")
    manual_version = manual_expenses_version(data_dir="data")
    account_csv_version = file_mtime(csv_path)
    refresh_nonce = int(st.session_state.get("_refresh_nonce", 0))

    # Background refresh: updates cache to today's date without blocking the UI.
    updater = fx_background_updater()
    if updater.error:
        st.caption(f"FX cache update warning: {updater.error}")

    if updater.done.is_set() and updater.updated and not st.session_state.get("_fx_cache_rerun_done"):
        st.session_state["_fx_cache_rerun_done"] = True
        st.rerun()

    tabs = st.tabs(["Expenses", "Investment"])

    with tabs[0]:
        if st.button("Refresh Expenses Data", key="refresh_expenses_data"):
            refresh_dashboard_data()

        prepared = load_prepared(
            csv_path,
            fx_version,
            manual_version,
            account_csv_version,
            refresh_nonce,
        )

        # Display max transaction date
        if not prepared.df.empty and "completed_date" in prepared.df.columns:
            max_date = pd.to_datetime(prepared.df["completed_date"], errors="coerce").max()
            if pd.notna(max_date):
                st.info(f"📅 Latest transaction: {max_date.strftime('%B %d, %Y at %H:%M')}")

        # Top-of-page budget progress for the current month
        plot_current_month_budget_progress(prepared.df)

        # If FX conversion fails for some rows (e.g., frankfurter timeout), those rows end up with amount_dkk = NA
        # and are excluded from totals/plots. Make this explicit so the dashboard stays trustworthy.
        df = prepared.df
        if not df.empty and {"type", "currency", "completed_date", "amount_net", "amount_dkk"}.issubset(df.columns):
            ccy = df["currency"].astype(str).str.upper().str.strip()
            relevant = (
                df["type"].isin(["income", "expense", "refund"])
                & df["completed_date"].notna()
                & df["amount_net"].notna()
                & ccy.ne("DKK")
            )
            missing = relevant & df["amount_dkk"].isna()
            if bool(missing.any()):
                summary = ccy[missing].value_counts().head(6)
                summary_txt = ", ".join([f"{k}: {int(v)}" for k, v in summary.items()])
                st.warning(
                    "FX conversion failed for some transactions (network/API timeout). "
                    "Those rows are excluded from monthly totals and plots. "
                    f"Missing conversions: {int(missing.sum())}. "
                    + (f"Top currencies: {summary_txt}" if summary_txt else "")
                )

        if prepared.spend_by_month_category.empty:
            st.warning("No expense rows with a valid DKK amount to plot.")
            return

        months = sorted(prepared.spend_by_month_category["month"].unique().tolist(), reverse=True)
        cross_month_summary = refund_cross_month_summary(prepared.df)

        # Three-column layout with row separators (newest month first)
        row_size = 3
        for row_start in range(0, len(months), row_size):
            row_months = months[row_start : row_start + row_size]
            cols = st.columns(row_size)

            for col_idx, m in enumerate(row_months):
                with cols[col_idx]:
                    plot_month(prepared.spend_by_month_category, prepared.totals_by_month, m)

                    exp_table = expenses_table_for_month(prepared.df, m)
                    if exp_table.empty:
                        st.caption("No expense rows for this month.")
                    else:
                        exp_total, inc_total, ref_total = month_totals(prepared.totals_by_month, m)
                        has_cross_month = cross_month_summary.get(m, False)
                        render_month_table_header(
                            exp_total,
                            inc_total,
                            ref_total,
                            items=len(exp_table),
                            has_cross_month=has_cross_month,
                        )

                        # Show 5 rows worth of height; scroll for the rest.
                        st.dataframe(
                            exp_table,
                            use_container_width=True,
                            height=290,
                            hide_index=True,
                        )

            # Distinct separation between this row of tables and the next row of charts.
            if row_start + row_size < len(months):
                st.markdown("<div style='margin: 0.4rem 0 0.2rem 0;'></div>", unsafe_allow_html=True)
                st.divider()
                st.markdown("<div style='margin: 0.2rem 0 0.6rem 0;'></div>", unsafe_allow_html=True)

        st.subheader("Expenses categorized as Other")
        render_other_expenses_editor(prepared.other_expenses.copy())

        st.divider()
        st.subheader("Manual external expenses")
        st.caption(
            "Manual imports are stored in data/manual_expenses.csv and included in all plots/tables."
        )

        with st.expander("Advanced", expanded=False):
            st.caption(
                "Add expenses from another bank account. Saved in data/manual_expenses.csv and automatically included in all plots/tables."
            )

            if st.session_state.get("_manual_expense_last_status") == "success":
                st.success("Success: manual expense saved.")
                st.session_state.pop("_manual_expense_last_status", None)

            with st.form("add_manual_expense", clear_on_submit=True):
                d = st.date_input("Date", help="Example: 2026-02-25")
                desc = st.text_input(
                    "Description",
                    placeholder="e.g., Dentist (external) / Mobile bill / Rent",
                    help="Free text shown in tables and used for categorization.",
                )
                amt_str = st.text_input(
                    "Amount (DKK)",
                    placeholder="e.g., 29.99",
                    help="Use dot for decimals (29.99). Comma (29,99) is also accepted and will be converted.",
                )
                cat = st.selectbox(
                    "Category (optional)",
                    options=[""] + category_options(),
                    help="Example: Groceries (leave empty to save as Other)",
                )
                submitted = st.form_submit_button("Add manual expense")

            if submitted:
                def parse_amount_dkk(raw: str) -> float | None:
                    s = str(raw or "").strip()
                    if not s:
                        return None
                    # Allow either decimal comma or dot; strip spaces.
                    s = s.replace(" ", "").replace(",", ".")
                    try:
                        return float(s)
                    except Exception:
                        return None

                errors: list[str] = []
                if not str(desc).strip():
                    errors.append("Description is required")

                amt = parse_amount_dkk(amt_str)
                if amt is None:
                    errors.append("Amount must be a number like 29.99")
                elif float(amt) <= 0:
                    errors.append("Amount must be > 0")

                if errors:
                    st.error("Failed: " + "; ".join(errors) + ".")
                else:
                    append_manual_expense(
                        data_dir="data",
                        completed_date=d,
                        description=str(desc),
                        amount_dkk=float(amt),
                        category=(str(cat).strip() or None),
                    )
                    st.session_state["_manual_expense_last_status"] = "success"
                    st.rerun()

    with tabs[1]:
        if st.button("Refresh Investment Data", key="refresh_investment_data"):
            refresh_dashboard_data()

        st.subheader("Investment FX P/L (DKK -> USD/GBP)")
        st.caption("Only exchange-out rows are included: sub_type=Exchange, description contains 'Exchanged to', currency=DKK, amount_net<0.")

        detail, totals, as_of = compute_exchange_fx_pnl(account_csv_path=csv_path, data_dir="data")
        st.caption(f"As of: {as_of.date()}")

        if detail.empty:
            st.info("No qualifying DKK->USD/GBP exchange transactions found in the account statement.")
            return

        st.divider()
        st.markdown("### Savings Interest Net of Fees")

        if savings_csv_path:
            st.caption(f"Savings CSV: {savings_csv_path}")
        else:
            st.info(f"No savings statement found: {savings_lookup_error or 'unknown error'}")
            return

        savings_detail, savings_totals = compute_savings_interest_summary(savings_csv_path)
        if savings_detail.empty or savings_totals.empty:
            st.info("No qualifying 'Interest PAID USD/GBP' rows were found in the savings statement.")
            return

        usd_net_dkk = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("USD"), "net_dkk"],
                errors="coerce",
            ).sum()
        )
        gbp_net_dkk = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("GBP"), "net_dkk"],
                errors="coerce",
            ).sum()
        )
        usd_net_foreign = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("USD"), "net_foreign"],
                errors="coerce",
            ).sum()
        )
        gbp_net_foreign = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("GBP"), "net_foreign"],
                errors="coerce",
            ).sum()
        )
        usd_interest_foreign = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("USD"), "interest_foreign"],
                errors="coerce",
            ).sum()
        )
        gbp_interest_foreign = float(
            pd.to_numeric(
                savings_totals.loc[savings_totals["currency"].eq("GBP"), "interest_foreign"],
                errors="coerce",
            ).sum()
        )
        usd_fee_foreign = abs(
            float(
                pd.to_numeric(
                    savings_totals.loc[savings_totals["currency"].eq("USD"), "fee_foreign"],
                    errors="coerce",
                ).sum()
            )
        )
        gbp_fee_foreign = abs(
            float(
                pd.to_numeric(
                    savings_totals.loc[savings_totals["currency"].eq("GBP"), "fee_foreign"],
                    errors="coerce",
                ).sum()
            )
        )
        dkk_net = float(pd.to_numeric(savings_totals["net_dkk"], errors="coerce").sum())

        today_fx_day = pd.Timestamp.today().normalize()
        usd_today_rate = _fx_rate_on_or_before(
            load_fx_cache_series("USD", data_dir="data", to_ccy=FX_CACHE_TO_CCY),
            today_fx_day,
        )
        gbp_today_rate = _fx_rate_on_or_before(
            load_fx_cache_series("GBP", data_dir="data", to_ccy=FX_CACHE_TO_CCY),
            today_fx_day,
        )
        usd_today_dkk = usd_net_foreign * float(usd_today_rate) if usd_today_rate is not None else np.nan
        gbp_today_dkk = gbp_net_foreign * float(gbp_today_rate) if gbp_today_rate is not None else np.nan
        usd_delta_dkk = usd_today_dkk - usd_net_dkk if pd.notna(usd_today_dkk) else np.nan
        gbp_delta_dkk = gbp_today_dkk - gbp_net_dkk if pd.notna(gbp_today_dkk) else np.nan

        def render_today_delta(today_value_dkk: float, delta_dkk: float) -> None:
            if pd.isna(today_value_dkk) or pd.isna(delta_dkk):
                st.caption("Today FX value unavailable")
                return
            color = "#22c55e" if delta_dkk > 0 else "#ef4444" if delta_dkk < 0 else "#9ca3af"
            sign = "+" if delta_dkk > 0 else ""
            st.markdown(
                (
                    "<div style='font-size:0.82rem; color:{color}; margin-top:-0.45rem;'>"
                    "Today: {today} DKK ({sign}{delta} DKK)"
                    "</div>"
                ).format(
                    color=color,
                    today=fmt_dkk(float(today_value_dkk)),
                    sign=sign,
                    delta=fmt_dkk(float(delta_dkk)),
                ),
                unsafe_allow_html=True,
            )

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("USD Net Interest", f"{fmt_dkk(usd_net_dkk)} DKK")
            st.caption(f"{usd_net_foreign:,.4f} USD")
            render_today_delta(usd_today_dkk, usd_delta_dkk)
        with c2:
            st.metric("GBP Net Interest", f"{fmt_dkk(gbp_net_dkk)} DKK")
            st.caption(f"{gbp_net_foreign:,.4f} GBP")
            render_today_delta(gbp_today_dkk, gbp_delta_dkk)
        with c3:
            combined_today = (usd_today_dkk if pd.notna(usd_today_dkk) else 0.0) + \
                             (gbp_today_dkk if pd.notna(gbp_today_dkk) else 0.0)
            combined_delta = combined_today - dkk_net
            delta_sign = "+" if combined_delta >= 0 else ""
            st.metric(
                "Net Interest in DKK",
                f"{fmt_dkk(combined_today)} DKK",
                delta=f"{delta_sign}{fmt_dkk(combined_delta)} DKK",
            )

        savings_detail_view = savings_detail.copy()
        savings_detail_view["interest_foreign"] = savings_detail_view.apply(
            lambda r: f"{float(r['interest_foreign']):,.4f} {r['currency']}", axis=1
        )
        savings_detail_view["fee_foreign"] = savings_detail_view.apply(
            lambda r: f"{float(r['fee_foreign']):,.4f} {r['currency']}", axis=1
        )
        savings_detail_view["net_foreign"] = savings_detail_view.apply(
            lambda r: f"{float(r['net_foreign']):,.4f} {r['currency']}", axis=1
        )
        savings_detail_view["fx_rate"] = savings_detail_view["fx_rate"].map(lambda x: f"{float(x):.4f}")
        savings_detail_view["net_dkk"] = savings_detail_view["net_dkk"].map(lambda x: fmt_dkk(float(x)))

        # --- Bar chart: Net Interest in DKK per month ---
        chart_df = savings_detail.copy()
        chart_df["month"] = pd.to_datetime(chart_df["datetime"]).dt.to_period("M")
        monthly = (
            chart_df.groupby("month", dropna=False)["net_dkk"]
            .sum()
            .reset_index()
            .sort_values("month", ascending=True)
        )
        monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")
        monthly["net_dkk"] = pd.to_numeric(monthly["net_dkk"], errors="coerce").fillna(0.0)

        n = len(monthly)
        bar_h = 0.28
        bg = "#0e1117"
        accent = "#22c55e"
        muted = "#94a3b8"

        fig_sav, ax_sav = plt.subplots(figsize=(4.2, max(1.15, n * 0.36 + 0.25)))
        fig_sav.patch.set_facecolor(bg)
        ax_sav.set_facecolor(bg)

        bars = ax_sav.barh(
            monthly["month_label"],
            monthly["net_dkk"],
            height=bar_h,
            color=accent,
            alpha=0.82,
            linewidth=0,
        )

        x_max = monthly["net_dkk"].max() * 1.14 if monthly["net_dkk"].max() > 0 else 1
        ax_sav.set_xlim(0, x_max)

        for bar, val in zip(bars, monthly["net_dkk"]):
            ax_sav.text(
                bar.get_width() + x_max * 0.015,
                bar.get_y() + bar.get_height() / 2,
                f"{val:,.0f}",
                va="center",
                ha="left",
                fontsize=7,
                color=muted,
                fontweight="normal",
            )

        ax_sav.set_xlabel("")
        ax_sav.set_title("Net Interest per Month", color="white", fontsize=9, pad=5, loc="left")
        ax_sav.tick_params(axis="y", colors="white", labelsize=7.5, length=0, pad=4)
        ax_sav.tick_params(axis="x", colors=muted, labelsize=6.5, length=0)
        ax_sav.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        for spine in ax_sav.spines.values():
            spine.set_visible(False)
        ax_sav.xaxis.grid(True, color="#1e293b", linewidth=0.45, alpha=0.8, zorder=0)
        ax_sav.set_axisbelow(True)

        plt.tight_layout(pad=0.35)
        col_chart, _ = st.columns([1.45, 1.55])
        with col_chart:
            st.pyplot(fig_sav)
        plt.close(fig_sav)

        total_dkk_out = float(pd.to_numeric(totals.get("dkk_out"), errors="coerce").sum())
        total_dkk_value_today = float(pd.to_numeric(totals.get("dkk_value_today"), errors="coerce").sum())
        total_pnl = total_dkk_value_today - total_dkk_out
        total_pct = (total_pnl / total_dkk_out) if total_dkk_out else 0.0

        st.metric(
            "Unrealised FX Gain/Loss on DKK→USD/GBP exchanges (at today's rate)",
            f"{fmt_dkk(total_pnl)} DKK",
            delta=f"{total_pct * 100:.2f}%",
        )

        totals_view = totals[["to_currency", "dkk_pnl", "pnl_pct"]].copy()
        totals_view = totals_view.rename(columns={"to_currency": "currency", "dkk_pnl": "gain_loss_dkk", "pnl_pct": "pnl_pct"})
        totals_view["gain_loss_dkk"] = totals_view["gain_loss_dkk"].map(lambda x: fmt_dkk(float(x)))
        totals_view["pnl_pct"] = totals_view["pnl_pct"].map(lambda x: f"{float(x) * 100:.2f}%")

        st.markdown("### By Currency")
        st.dataframe(totals_view, use_container_width=True, hide_index=True)

        detail_view = detail.copy()
        detail_view = detail_view.rename(
            columns={
                "completed_date": "datetime",
                "to_currency": "to_ccy",
                "dkk_out": "dkk_exchanged",
                "fx_at_exchange": "fx_dkk_per_ccy_at_trade",
                "foreign_bought": "ccy_bought",
                "fx_today": "fx_dkk_per_ccy_today",
                "dkk_value_today": "dkk_value_today",
                "dkk_pnl": "dkk_gain_loss",
                "pnl_pct": "pnl_pct",
            }
        )
        detail_view["pnl_pct"] = detail_view["pnl_pct"].map(lambda x: f"{float(x) * 100:.2f}%" if pd.notna(x) else "")



if __name__ == "__main__":
    main()


