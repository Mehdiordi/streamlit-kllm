import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timedelta
import boto3
import io
from botocore.exceptions import ClientError

st.set_page_config(page_title="Simple Budget Tracker", layout="wide")
st.markdown("<h1 style='margin:0 0 8px 0'>🏠 Simple Budget Tracker</h1>", unsafe_allow_html=True)
st.write("A minimal, clear view of monthly & weekly spending vs limits. Upload your CSV or point to a local path.")

# Sidebar - data source and limits
st.sidebar.header("Data / Limits")
default_path = Path("data/20250928_transaction-history_wise.csv")
csv_path = st.sidebar.text_input("Local CSV path (used if not uploading)", value=str(default_path))

monthly_limit = st.sidebar.number_input("Monthly limit (DKK)", min_value=0, value=18000, step=500, format="%d")
# default weekly ~ month / 4.33
weekly_default = float(monthly_limit) / 4.33
weekly_limit = st.sidebar.number_input("Weekly limit (DKK)", min_value=0.0, value=round(weekly_default, 2), step=100.0, format="%.2f")

st.sidebar.markdown("---")
st.sidebar.write("FX rates (used to convert to DKK if your CSV has a currency column)")
fx_dkk = st.sidebar.number_input("EUR → DKK", value=7.46, format="%.4f")
fx_usd = st.sidebar.number_input("USD → DKK", value=6.85, format="%.4f")
FX_MAP = {"DKK": 1.0, "EUR": float(fx_dkk), "USD": float(fx_usd)}

# -----------------------------------------------------------------------------
# CSV loading helpers: uploader -> local path -> S3 (using st.secrets)
# -----------------------------------------------------------------------------
import io
import boto3
from botocore.exceptions import ClientError

@st.cache_data
def load_csv_from_buffer(buf):
    return pd.read_csv(buf)

def load_csv_from_s3(bucket: str, key: str, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return pd.read_csv(io.BytesIO(body))

# Determine data source (uploader prioritized, then local path, then S3 via secrets)
use_uploader = st.sidebar.checkbox("Upload CSV", value=False)
uploaded_file = st.sidebar.file_uploader("Transaction CSV", type=["csv"]) if use_uploader else None
# csv_path sidebar input is already present earlier

df_raw = None
data_source = "None"

if use_uploader and uploaded_file is not None:
    try:
        df_raw = load_csv_from_buffer(uploaded_file)
        data_source = f"Uploaded: {uploaded_file.name}"
    except Exception as e:
        st.error(f"Failed to read uploaded CSV: {e}")
        st.stop()
else:
    try:
        # try local path first
        if csv_path and Path(csv_path).exists():
            df_raw = load_csv_from_buffer(csv_path)
            data_source = f"Local path: {csv_path}"
        else:
            # fallback to S3 (expects secrets set: AWS_*, S3_BUCKET, S3_KEY)
            s3_bucket = st.secrets.get("S3_BUCKET")
            s3_key = st.secrets.get("S3_KEY")
            aws_id = st.secrets.get("AWS_ACCESS_KEY_ID")
            aws_secret = st.secrets.get("AWS_SECRET_ACCESS_KEY")
            aws_region = st.secrets.get("AWS_REGION", None)

            if s3_bucket and s3_key:
                try:
                    df_raw = load_csv_from_s3(
                        bucket=s3_bucket,
                        key=s3_key,
                        aws_access_key_id=aws_id,
                        aws_secret_access_key=aws_secret,
                        region_name=aws_region,
                    )
                    data_source = f"S3: s3://{s3_bucket}/{s3_key}"
                except ClientError as ce:
                    st.error(f"S3 access error: {ce}")
                    st.stop()
                except Exception as e:
                    st.error(f"Failed to read CSV from S3: {e}")
                    st.stop()
            else:
                st.error("No valid CSV found: local path doesn't exist and S3 secrets/key not configured.")
                st.stop()
    except Exception as e:
        st.error(f"Failed to read local CSV: {e}")
        st.stop()

if df_raw is None:
    st.error("Could not load CSV from any source.")
    st.stop()

st.caption(data_source)
# normalize column names
df_raw.columns = [c.strip() for c in df_raw.columns]

# --- Identify date / amount / currency / direction / counterparty columns (robust)
date_candidates = ["Created on", "Finished on", "created_on", "finished_on", "date", "Date"]
amt_candidates = ["Target amount (after fees)", "Amount", "amount", "Value"]
cur_candidates = ["Target currency", "Currency", "currency"]
dir_candidates = ["Direction", "direction", "Type", "type"]
cp_candidates = ["Target name", "Merchant", "merchant", "Counterparty", "counterparty", "Name"]

date_col = next((c for c in date_candidates if c in df_raw.columns), None)
amt_col = next((c for c in amt_candidates if c in df_raw.columns), None)
cur_col = next((c for c in cur_candidates if c in df_raw.columns), None)
dir_col = next((c for c in dir_candidates if c in df_raw.columns), None)
cp_col = next((c for c in cp_candidates if c in df_raw.columns), None)

if date_col is None or amt_col is None:
    st.error("Could not find required columns (date and amount). Please ensure CSV has a date and an amount column.")
    st.stop()

# Parse date column
df_raw[date_col] = pd.to_datetime(df_raw[date_col], errors="coerce")
# unified amount: positive income, negative expense (best effort using direction if present)
def unified_amount(row):
    try:
        amt = float(row[amt_col])
    except Exception:
        return np.nan
    # apply direction if present
    if dir_col and pd.notna(row.get(dir_col)):
        d = str(row.get(dir_col)).upper().strip()
        if d.startswith("OUT") or d.startswith("DEBIT") or d.startswith("PAYMENT"):
            amt = -abs(amt)
        elif d.startswith("IN") or d.startswith("CREDIT"):
            amt = abs(amt)
    return amt

df = df_raw.copy()
df["date"] = df[date_col]
df["amount"] = df.apply(unified_amount, axis=1)
df["currency"] = df[cur_col] if cur_col else "DKK"
df["counterparty"] = df[cp_col] if cp_col else "Unknown"

# Drop invalid rows
df = df[~df["date"].isna()].copy()
df = df[~df["amount"].isna()].copy()

if df.empty:
    st.warning("No valid rows after parsing date/amount.")
    st.stop()

# Convert all amounts to DKK for consistent comparison
def to_dkk(row):
    cur = row.get("currency", "DKK")
    try:
        rate = FX_MAP.get(cur, 1.0)
    except Exception:
        rate = 1.0
    return row["amount"] * float(rate)

df["amount_dkk"] = df.apply(to_dkk, axis=1)
# Consider expenses as absolute of negative amounts
df["expense_dkk"] = df["amount_dkk"].apply(lambda x: abs(x) if x < 0 else 0.0)

# Reference dates
last_date = df["date"].max().normalize()
today = pd.Timestamp.now().normalize()

# Compute month period that contains last_date
month_start = last_date.replace(day=1)
month_end = last_date  # up to last_date present in CSV

# Calendar week: Monday-based week that contains last_date
week_start = (last_date - pd.Timedelta(days=int(last_date.weekday()))).normalize()
week_end = last_date

# Totals
spent_month_to_last_date = df.loc[(df["date"] >= month_start) & (df["date"] <= month_end), "expense_dkk"].sum()
spent_week_to_last_date = df.loc[(df["date"] >= week_start) & (df["date"] <= week_end), "expense_dkk"].sum()

# Also show "till today" totals (if CSV contains entries up to today, it'll be same)
spent_month_to_today = df.loc[(df["date"] >= today.replace(day=1)) & (df["date"] <= today), "expense_dkk"].sum() if today <= last_date else spent_month_to_last_date
spent_week_to_today = df.loc[(df["date"] >= (today - pd.Timedelta(days=int(today.weekday())))) & (df["date"] <= today), "expense_dkk"].sum() if today <= last_date else spent_week_to_last_date

# Comparison computations
month_remaining = monthly_limit - spent_month_to_last_date
week_remaining = weekly_limit - spent_week_to_last_date

month_pct = min(max(spent_month_to_last_date / monthly_limit, 0), 2) if monthly_limit > 0 else 0
week_pct = min(max(spent_week_to_last_date / weekly_limit, 0), 2) if weekly_limit > 0 else 0

# Layout: top metrics
col1, col2, col3 = st.columns([1.2, 1.2, 1])
with col1:
    st.subheader("This month (to last CSV date)")
    label = f"{month_start.strftime('%Y-%m-%d')} → {month_end.strftime('%Y-%m-%d')}"
    delta = f"{'-' if month_remaining>=0 else '+'}{abs(month_remaining):,.0f} DKK"
    st.metric(label="Spent", value=f"{spent_month_to_last_date:,.0f} DKK", delta=delta)
    st.progress(min(spent_month_to_last_date / monthly_limit if monthly_limit>0 else 0, 1.0))

with col2:
    st.subheader("This week (to last CSV date)")
    labelw = f"{week_start.strftime('%Y-%m-%d')} → {week_end.strftime('%Y-%m-%d')}"
    deltaw = f"{'-' if week_remaining>=0 else '+'}{abs(week_remaining):,.0f} DKK"
    st.metric(label="Spent", value=f"{spent_week_to_last_date:,.0f} DKK", delta=deltaw)
    st.progress(min(spent_week_to_last_date / weekly_limit if weekly_limit>0 else 0, 1.0))

with col3:
    st.subheader("Reference dates")
    st.write(f"Last date in CSV: **{last_date.date()}**")
    st.write(f"Today: **{today.date()}**")
    st.caption("All calculations use CSV data up to the last date present in the file.")

st.markdown("---")

# Simple comparison bars (Plotly) — improved: limit line + spent (green + red overage)
comp_df = pd.DataFrame({
    "scope": ["Month (to last CSV date)", "Week (to last CSV date)"],
    "spent": [spent_month_to_last_date, spent_week_to_last_date],
    "limit": [monthly_limit, weekly_limit]
})

# split spent into "within limit" and "over limit" parts
spent_up = [min(s, l) for s, l in zip(comp_df["spent"], comp_df["limit"])]
spent_over = [max(s - l, 0) for s, l in zip(comp_df["spent"], comp_df["limit"])]

fig = go.Figure()

# background "limit" thin bar (light/blurred)
fig.add_trace(go.Bar(
    y=comp_df["scope"],
    x=comp_df["limit"],
    orientation="h",
    name="Limit",
    marker=dict(color="rgba(120,120,120,0.20)"),
    showlegend=False,
    hovertemplate="Limit: %{x:,.0f} DKK<extra></extra>"
))

# spent within the limit (green)
fig.add_trace(go.Bar(
    y=comp_df["scope"],
    x=spent_up,
    orientation="h",
    name="Spent (≤ limit)",
    marker=dict(color="#2E8B57"),
    text=[f"{v:,.0f} DKK" if v>0 else "" for v in comp_df["spent"]],
    textposition="inside",
    insidetextanchor="middle",
    hovertemplate="Spent (within): %{x:,.0f} DKK<extra></extra>"
))

# over-limit portion (red) - we place it starting at the end of the within-limit segment using 'base'
fig.add_trace(go.Bar(
    y=comp_df["scope"],
    x=spent_over,
    orientation="h",
    name="Over limit",
    marker=dict(color="#D9534F"),
    base=spent_up,
    text=["" if v==0 else f"{v:,.0f} DKK" for v in spent_over],
    textposition="inside",
    insidetextanchor="middle",
    hovertemplate="Overage: %{x:,.0f} DKK<extra></extra>"
))

fig.update_layout(
    title_text="Spent vs Limit (to last CSV date)",
    barmode="overlay",
    height=300,
    margin=dict(t=40, l=40, r=40, b=20),
    xaxis_title="DKK",
    yaxis=dict(autorange="reversed")  # keep Month on top
)

# show also numeric totals in the subtitle area by annotation (optional)
for i, row in comp_df.iterrows():
    tot = row["spent"]
    lim = row["limit"]
    status = "OK" if tot <= lim else "OVER"
    color = "#2E8B57" if tot <= lim else "#D9534F"
    fig.add_annotation(
        x=tot,
        y=row["scope"],
        xanchor="left",
        yanchor="middle",
        text=f"{tot:,.0f} DKK ({status})",
        showarrow=False,
        font=dict(color=color, size=12)
    )

st.plotly_chart(fig, use_container_width=True)

# Daily cumulative chart for current month (up to last_date)
month_days = pd.date_range(start=month_start, end=month_end, freq='D')
daily = df.loc[(df["date"] >= month_start) & (df["date"] <= month_end)].copy()
daily_sum = daily.groupby(daily["date"].dt.normalize())["expense_dkk"].sum().reindex(month_days, fill_value=0).cumsum()
daily_df = pd.DataFrame({"date": month_days, "cumulative_spent": daily_sum.values})
daily_df["limit_progress"] = [(i+1)/len(month_days) * monthly_limit for i in range(len(month_days))]

fig2 = px.line(daily_df, x="date", y="cumulative_spent", title="Cumulative spending this month (up to last CSV date)", labels={"cumulative_spent":"Cumulative spent (DKK)"})
fig2.add_scatter(x=daily_df["date"], y=daily_df["limit_progress"], mode="lines", name="Pro-rated limit", line=dict(dash="dash", color="#FFA500"))
fig2.update_layout(height=360, margin=dict(t=40,l=40,r=40,b=20))
st.plotly_chart(fig2, use_container_width=True)

# Helpful textual summary
st.markdown("### Summary")
if month_remaining >= 0:
    st.success(f"You are under the monthly limit by {month_remaining:,.0f} DKK (based on data up to {last_date.date()}).")
else:
    st.error(f"You are OVER the monthly limit by {abs(month_remaining):,.0f} DKK (based on data up to {last_date.date()}).")

if week_remaining >= 0:
    st.success(f"You are under the weekly limit by {week_remaining:,.0f} DKK (week starting {week_start.date()}).")
else:
    st.error(f"You are OVER the weekly limit by {abs(week_remaining):,.0f} DKK (week starting {week_start.date()}).")

st.markdown("---")
st.caption("Notes: Expenses are inferred from negative amounts in the CSV. If you have multiple currencies, edit FX rates in the sidebar.")

# Always show top counterparties for current month + last 3 months
st.markdown("---")
st.header("Top counterparties — Current month + last 3 months")

# Build the 4 monthly periods (reference = last_date from CSV)
last_period = last_date.to_period("M")
periods = [last_period - i for i in range(0, 4)]  # last_period, last-1, last-2, last-3
# Show oldest -> newest (left -> right)
periods = list(reversed(periods))

cols = st.columns(4)
for i, period in enumerate(periods):
    col = cols[i]
    # Filter rows for this period
    mask = df["date"].dt.to_period("M") == period
    month_df = df.loc[mask].copy()
    display_label = period.strftime("%b %Y")  # e.g. "Oct 2025"
    with col:
        st.subheader(display_label)
        if month_df.empty:
            st.write("No data")
            continue

        # Summarize top counterparties by expense (DKK)
        top_cp = (
            month_df.groupby("counterparty")["expense_dkk"]
            .sum()
            .abs()
            .sort_values(ascending=False)
            .head(8)
        )

        # Prepare table for display (formatted)
        table_df = top_cp.rename("Total expense (DKK)").to_frame()
        table_df["Total expense (DKK)"] = table_df["Total expense (DKK)"].round(0).astype(int)
        # Show small table
        st.table(table_df)

        # Mini horizontal bar chart for quick visual
        try:
            plot_df = table_df.reset_index().rename(columns={"counterparty": "Counterparty", "Total expense (DKK)": "Amount"})
            if not plot_df.empty:
                fig_small = px.bar(
                    plot_df,
                    x="Amount",
                    y="Counterparty",
                    orientation="h",
                    text="Amount",
                    color_discrete_sequence=["#2E8B57"],
                )
                fig_small.update_traces(texttemplate="%{text:,}", textposition="inside")
                fig_small.update_layout(
                    height=220,
                    margin=dict(t=10, l=10, r=10, b=10),
                    xaxis_title="DKK",
                    yaxis=dict(autorange="reversed")  # keep largest on top
                )
                st.plotly_chart(fig_small, use_container_width=True)
        except Exception:
            # If plotting fails, skip gracefully
            pass

# End of file