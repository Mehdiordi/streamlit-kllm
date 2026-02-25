from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from fx_cache import FxCacheBackgroundUpdater, ensure_fx_cache_files, fx_cache_version
from fx_cache import FX_CACHE_TO_CCY, load_fx_cache_series
import invest_processing as inv
from processing import (
    PreparedData,
    append_manual_expense,
    cleanup_outdated_account_statement_csvs,
    find_latest_account_statement_csv,
    load_expense_category_map,
    load_manual_expenses,
    load_monthly_limits,
    prepare_data_for_plotting,
)


def fmt_dkk(x: float) -> str:
    return f"{x:,.0f}"


@st.cache_data(show_spinner=True)
def load_prepared(csv_path: str, fx_version: float, manual_version: float) -> PreparedData:
    # manual_version exists purely to invalidate the cache when manual_expenses.csv changes
    _ = manual_version
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
    }


@st.cache_data(show_spinner=False)
def category_options() -> list[str]:
    # unique categories from YAML mapping
    compiled = load_expense_category_map()
    cats = sorted({cat for _kw, cat in compiled if cat})
    return cats


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


def render_month_table_header(exp_total: float, inc_total: float, ref_total: float, items: int) -> None:
    # Compact caption-style header (small text) like: 💸 5 DKK |  💰 0 DKK |  ♻️ 0 DKK | 📊 1
    st.markdown(
        "<small>"
        f"💸 <b>{fmt_dkk(exp_total)}</b> DKK | "
        f"💰 {fmt_dkk(inc_total)} DKK | "
        f"♻️ {fmt_dkk(ref_total)} DKK | "
        f"📊 {items}"
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

    ax.set_title("Cumulative spending (DKK)", color=fg, fontsize=11, fontweight="bold", pad=8)
    ax.tick_params(axis="x", colors=fg, labelsize=8)
    ax.tick_params(axis="y", colors=fg, labelsize=8)
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.grid(True, axis="y", color=grid, alpha=0.35, linewidth=0.8)
    ax.set_axisbelow(True)

    # Keep y-axis starting at 0 for readability
    max_y = float(np.nanmax([allowed_cum.max(), actual_cum.max()])) if len(days) else 0.0
    ax.set_ylim(0, max(1.0, max_y * 1.08))

    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


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
    manual_version = manual_expenses_version(data_dir="data")

    # Background refresh: updates cache to today's date without blocking the UI.
    updater = fx_background_updater()
    if updater.error:
        st.caption(f"FX cache update warning: {updater.error}")

    if updater.done.is_set() and updater.updated and not st.session_state.get("_fx_cache_rerun_done"):
        st.session_state["_fx_cache_rerun_done"] = True
        st.rerun()

    tabs = st.tabs(["Expenses", "Investment"])

    with tabs[0]:
        prepared = load_prepared(csv_path, fx_version, manual_version)

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
        st.subheader("Investment")

        try:
            consolidated_csv_path = inv.find_latest_consolidated_statement_csv("data")
        except Exception as e:
            st.error(f"Missing consolidated statement CSV: {e}")
            return

        st.caption(f"Account CSV: {csv_path}")
        st.caption(f"Investment CSV: {consolidated_csv_path}")

        summary_data = load_investment_summary(
            account_csv_path=csv_path,
            consolidated_csv_path=consolidated_csv_path,
            fx_version=fx_version,
            account_version=file_mtime(csv_path),
            consolidated_version=file_mtime(consolidated_csv_path),
        )

        summary_df = summary_data.get("summary")
        today = summary_data.get("today")

        if isinstance(today, pd.Timestamp):
            st.caption(f"As of: {today.date()}")

        if not isinstance(summary_df, pd.DataFrame) or summary_df.empty:
            st.info("No summary data found in consolidated statement.")
            return

        # Extract key metrics by section and convert to DKK
        metrics = {}
        fx_rates = {}
        
        # Get FX rates for today
        for ccy in ["USD", "GBP"]:
            s = load_fx_cache_series(ccy, data_dir="data", to_ccy=FX_CACHE_TO_CCY)
            fx_rates[ccy] = float(s.get(pd.Timestamp.today().normalize())) if not s.empty else None
        
        # Parse summary data by section
        for section in summary_df["section"].unique():
            section_data = summary_df[summary_df["section"] == section]
            metrics[section] = {}
            for _, row in section_data.iterrows():
                desc = row["description"]
                metrics[section][desc] = {
                    "value": row["value"],
                    "currency": row["currency"],
                    "amount_str": row["amount"]
                }

        # === GBP Cash Funds Table ===
        gbp_rows = []
        gbp_dkk_totals = {}
        if "Flexible Cash Funds - GBP" in metrics:
            st.markdown("### GBP Cash Funds")
            gbp_data = metrics["Flexible Cash Funds - GBP"]
            
            for desc, data in gbp_data.items():
                val = data.get("value", 0) or 0
                ccy = data.get("currency", "GBP")
                dkk_val = val * fx_rates.get("GBP", 0) if fx_rates.get("GBP") else 0
                gbp_rows.append({
                    "Description": desc,
                    f"Amount ({ccy})": f"{val:,.2f}",
                    "Value (DKK)": dkk_val
                })
                gbp_dkk_totals[desc] = dkk_val
            
            gbp_df = pd.DataFrame(gbp_rows)
            gbp_df["Value (DKK)"] = gbp_df["Value (DKK)"].apply(fmt_dkk)
            st.dataframe(gbp_df, use_container_width=True, hide_index=True)
        
        # === USD Cash Funds Table ===
        usd_rows = []
        usd_dkk_totals = {}
        if "Flexible Cash Funds - USD" in metrics:
            st.markdown("### USD Cash Funds")
            usd_data = metrics["Flexible Cash Funds - USD"]
            
            for desc, data in usd_data.items():
                val = data.get("value", 0) or 0
                ccy = data.get("currency", "USD")
                dkk_val = val * fx_rates.get("USD", 0) if fx_rates.get("USD") else 0
                usd_rows.append({
                    "Description": desc,
                    f"Amount ({ccy})": f"{val:,.2f}",
                    "Value (DKK)": dkk_val
                })
                usd_dkk_totals[desc] = dkk_val
            
            usd_df = pd.DataFrame(usd_rows)
            usd_df["Value (DKK)"] = usd_df["Value (DKK)"].apply(fmt_dkk)
            st.dataframe(usd_df, use_container_width=True, hide_index=True)

        # === Combined Summary Table (DKK) ===
        st.markdown("### Combined Summary (DKK)")
        
        # Get all unique descriptions from both tables
        all_descriptions = set()
        if gbp_dkk_totals:
            all_descriptions.update(gbp_dkk_totals.keys())
        if usd_dkk_totals:
            all_descriptions.update(usd_dkk_totals.keys())
        
        summary_rows = []
        combined_metrics = {}  # For saving to history
        for desc in sorted(all_descriptions):
            gbp_val = gbp_dkk_totals.get(desc, 0)
            usd_val = usd_dkk_totals.get(desc, 0)
            total_val = gbp_val + usd_val
            summary_rows.append({
                "Description": desc,
                "GBP (DKK)": fmt_dkk(gbp_val),
                "USD (DKK)": fmt_dkk(usd_val),
                "Total (DKK)": fmt_dkk(total_val)
            })
            combined_metrics[desc] = total_val
        
        if summary_rows:
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
        
        # === Save snapshot to history ===
        snapshot_date = inv.extract_end_date_from_filename(consolidated_csv_path)
        if snapshot_date is not None and combined_metrics:
            try:
                inv.save_investment_snapshot(
                    snapshot_date=snapshot_date,
                    summary_metrics=combined_metrics,
                    history_file="data/investment_history.csv"
                )
            except Exception as e:
                st.warning(f"Could not save investment snapshot: {e}")
        
        # === Portfolio Growth Chart ===
        try:
            history_df = inv.load_investment_history("data/investment_history.csv")
            
            if not history_df.empty and len(history_df) >= 4 and "Closing balance" in history_df.columns:
                st.markdown("### Portfolio Growth Over Time")
                
                # Prepare data for plotting
                plot_df = history_df[['date', 'Closing balance']].dropna()
                
                if len(plot_df) >= 4:
                    fig, ax = plt.subplots(figsize=(12, 6))
                    
                    # Plot line with markers
                    ax.plot(plot_df['date'], plot_df['Closing balance'], 
                           marker='o', linewidth=2, markersize=8, 
                           color='#4ECDC4', label='Closing Balance')
                    
                    # Format
                    ax.set_xlabel('Date', fontsize=12)
                    ax.set_ylabel('Portfolio Value (DKK)', fontsize=12)
                    ax.set_title('Portfolio Closing Balance Over Time', fontsize=14, weight='bold')
                    ax.grid(True, alpha=0.3)
                    ax.legend()
                    
                    # Format y-axis with thousands separator
                    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
                    
                    # Rotate x-axis labels for better readability
                    plt.xticks(rotation=45, ha='right')
                    
                    # Tight layout to prevent label cutoff
                    plt.tight_layout()
                    
                    st.pyplot(fig)
                    plt.close(fig)
                    
                    # Show growth statistics
                    if len(plot_df) >= 2:
                        first_val = plot_df.iloc[0]['Closing balance']
                        last_val = plot_df.iloc[-1]['Closing balance']
                        change = last_val - first_val
                        pct_change = (change / first_val * 100) if first_val != 0 else 0
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Starting Balance", f"{fmt_dkk(first_val)} DKK")
                        with col2:
                            st.metric("Current Balance", f"{fmt_dkk(last_val)} DKK")
                        with col3:
                            st.metric("Total Growth", f"{fmt_dkk(change)} DKK", 
                                     delta=f"{pct_change:.2f}%")
                else:
                    st.info(f"Portfolio growth chart will appear after recording 4 snapshots (currently: {len(plot_df)})")
            else:
                st.info("Portfolio growth chart will appear after recording 4 snapshots with closing balance data.")
        except Exception as e:
            st.warning(f"Could not load investment history: {e}")


if __name__ == "__main__":
    main()


