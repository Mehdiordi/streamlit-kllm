from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from processing import PreparedData, prepare_data_for_plotting


CSV_PATH_DEFAULT = "data/account-statement_2025-11-28_2026-02-07_en-us_a19b3a.csv"


def fmt_dkk(x: float) -> str:
    return f"{x:,.0f}"


@st.cache_data(show_spinner=True)
def load_prepared(csv_path: str) -> PreparedData:
    return prepare_data_for_plotting(csv_path)


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

    title = (
        f"{month_label} "
        f"EXPENSE {fmt_dkk(exp_total)} | "
        f"INCOME {fmt_dkk(inc_total)} | "
        f"REFUND {fmt_dkk(ref_total)}"
    )

    fig_h = max(3.5, 0.35 * len(s))
    fig, ax = plt.subplots(figsize=(10, fig_h))
    ax.barh(s.index.astype(str), s.values)
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel("DKK")
    ax.set_ylabel("Category")
    ax.set_xlim(left=0)
    ax.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def month_totals(totals_by_month: pd.DataFrame, month: str) -> tuple[float, float, float]:
    exp_total = float(totals_by_month.loc[month, "expense"]) if month in totals_by_month.index else 0.0
    inc_total = float(totals_by_month.loc[month, "income"]) if month in totals_by_month.index else 0.0
    ref_total = float(totals_by_month.loc[month, "refund"]) if month in totals_by_month.index else 0.0
    return exp_total, inc_total, ref_total


def render_month_table_header(exp_total: float, inc_total: float, ref_total: float, items: int) -> None:
    # Compact caption-style header (small text) like: 🐝 5 DKK | 🥇 0 DKK | 🟢 0 DKK | 📊 1
    st.caption(
        f"🐝 {fmt_dkk(exp_total)} DKK | 🥇 {fmt_dkk(inc_total)} DKK | 🟢 {fmt_dkk(ref_total)} DKK | 📊 {items}"
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

    st.title("Revolut account statement – quick view")

    csv_path = CSV_PATH_DEFAULT
    st.caption(f"CSV: {csv_path}")

    prepared = load_prepared(csv_path)

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
                    height=210,
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
