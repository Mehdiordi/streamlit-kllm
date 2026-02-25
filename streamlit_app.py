from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from fx_cache import FxCacheBackgroundUpdater, ensure_fx_cache_files, fx_cache_version
from processing import (
    PreparedData,
    cleanup_outdated_account_statement_csvs,
    find_latest_account_statement_csv,
    prepare_data_for_plotting,
)


def fmt_dkk(x: float) -> str:
    return f"{x:,.0f}"


@st.cache_data(show_spinner=True)
def load_prepared(csv_path: str, fx_version: float) -> PreparedData:
    return prepare_data_for_plotting(csv_path)


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

    fig_h = max(3.0, 0.33 * len(s))
    fig, ax = plt.subplots(figsize=(5.8, fig_h), dpi=120)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    bars = ax.barh(s.index.astype(str), s.values, color=bar)
    ax.invert_yaxis()
    ax.set_title(title, loc="left", fontsize=10.5, color=fg, fontweight="bold", pad=6)
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

    # Value labels (on/near the end of each bar)
    pad_inside = max_val * 0.02
    pad_outside = max_val * 0.015
    for b in bars:
        w = float(b.get_width())
        y = float(b.get_y() + b.get_height() / 2)
        label = fmt_dkk(w)
        if max_val > 0 and w >= max_val * 0.12:
            ax.text(
                w - pad_inside,
                y,
                label,
                va="center",
                ha="right",
                color="#fde047",  # yellow
                fontsize=8.5,
                fontweight="bold",
            )
        else:
            ax.text(
                w + pad_outside,
                y,
                label,
                va="center",
                ha="left",
                color=fg,
                fontsize=8.5,
                fontweight="bold",
            )

    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def month_totals(totals_by_month: pd.DataFrame, month: str) -> tuple[float, float, float]:
    exp_total = float(totals_by_month.loc[month, "expense"]) if month in totals_by_month.index else 0.0
    inc_total = float(totals_by_month.loc[month, "income"]) if month in totals_by_month.index else 0.0
    ref_total = float(totals_by_month.loc[month, "refund"]) if month in totals_by_month.index else 0.0
    return exp_total, inc_total, ref_total


def render_month_table_header(exp_total: float, inc_total: float, ref_total: float, items: int) -> None:
    # Compact caption-style header (small text) like: 💸 5 DKK |  💰 0 DKK |  ♻️ 0 DKK | 📊 1
    st.caption(
        f"💸 {fmt_dkk(exp_total)} DKK | 💰 {fmt_dkk(inc_total)} DKK | ♻️ {fmt_dkk(ref_total)} DKK | 📊 {items}"
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


def main():
    st.set_page_config(page_title="Revolut expenses", layout="wide")

    st.title("Revolut statement")

    try:
        csv_path = find_latest_account_statement_csv("data")
    except Exception as e:
        st.error(str(e))
        return

    # Keep workspace tidy: delete older account-statement CSVs.
    cleanup_outdated_account_statement_csvs("data", keep_path=csv_path)

    st.caption(f"CSV: {csv_path}")

    # FX cache: first run will download and build local CSVs (USD/EUR/GBP->DKK) which can take a bit.
    with st.spinner("Preparing FX cache (first run may take a bit)…"):
        ensure_fx_cache_files(data_dir="data")

    fx_version = fx_cache_version(data_dir="data")

    # Background refresh: updates cache to today's date without blocking the UI.
    updater = fx_background_updater()
    if updater.error:
        st.caption(f"FX cache update warning: {updater.error}")

    if updater.done.is_set() and updater.updated and not st.session_state.get("_fx_cache_rerun_done"):
        st.session_state["_fx_cache_rerun_done"] = True
        st.rerun()

    prepared = load_prepared(csv_path, fx_version)

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

    # Three-column layout (newest month first)
    cols = st.columns(3)
    for idx, m in enumerate(months):
        with cols[idx % 3]:
            plot_month(prepared.spend_by_month_category, prepared.totals_by_month, m)

            exp_table = expenses_table_for_month(prepared.df, m)
            if exp_table.empty:
                st.caption("No expense rows for this month.")
            else:
                exp_total, inc_total, ref_total = month_totals(prepared.totals_by_month, m)
                render_month_table_header(exp_total, inc_total, ref_total, items=len(exp_table))

                # Show 5 rows worth of height; scroll for the rest.
                st.dataframe(
                    exp_table,
                    use_container_width=True,
                    height=290,
                    hide_index=True,
                )

    st.subheader("Expenses categorized as Other")
    other_df = prepared.other_expenses.copy()
    if other_df.empty:
        st.write("No expense rows categorized as 'Other'.")
    else:
        other_df = other_df.reset_index().rename(columns={"index": "row"})

        # Coerce common types for readability and sorting
        for c in ["completed_date", "started_date"]:
            if c in other_df.columns:
                other_df[c] = pd.to_datetime(other_df[c], errors="coerce")

        for c in ["amount", "fee", "amount_net", "conversion_rate", "amount_dkk", "balance"]:
            if c in other_df.columns:
                other_df[c] = pd.to_numeric(other_df[c], errors="coerce")

        # Default sort: highest spend first.
        # Prefer abs(amount_dkk); if missing (e.g., missing completed_date), fall back to abs(amount_net).
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

        st.dataframe(
            other_df[cols],
            use_container_width=True,
            height=210,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
