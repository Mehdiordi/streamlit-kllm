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

# Import personal categories for expense categorization
try:
    from config import personal_categories
except ImportError:
    personal_categories = {}  # Fallback if config.py doesn't exist

st.set_page_config(page_title=" Budget Tracker", layout="wide")
st.markdown("<h1 style='margin:0 0 8px 0'>🏠 Budget Tracker</h1>", unsafe_allow_html=True)

st.sidebar.header("Data / Limits")
default_path = Path("data/transaction-history.csv")
csv_path = st.sidebar.text_input("Local CSV path (used if not uploading)", value=str(default_path))

monthly_limit = st.sidebar.number_input("Monthly limit (DKK)", min_value=0, value=18000, step=500, format="%d")
# default weekly ~ month / 4.33
weekly_default = float(monthly_limit) / 4.33
weekly_limit = st.sidebar.number_input("Weekly limit (DKK)", min_value=0.0, value=round(weekly_default, 2), step=100.0, format="%.2f")

st.sidebar.markdown("---")
st.sidebar.write("FX rates (used to convert to DKK if your data has a currency column)")
fx_dkk = st.sidebar.number_input("EUR → DKK", value=7.44, format="%.4f")
fx_usd = st.sidebar.number_input("USD → DKK", value=6.35, format="%.4f")
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

# Determine data source (uploader prioritized, then S3 if configured, then local path)
use_uploader = st.sidebar.checkbox("Upload CSV", value=False)
uploaded_file = st.sidebar.file_uploader("Transaction CSV", type=["csv"]) if use_uploader else None

df_raw = None
data_source = "None"

# read S3 secrets now to decide precedence
s3_bucket = st.secrets.get("S3_BUCKET")
s3_key = st.secrets.get("S3_KEY", "data/home_expenses.csv")
aws_id = st.secrets.get("AWS_ACCESS_KEY_ID")
aws_secret = st.secrets.get("AWS_SECRET_ACCESS_KEY")
aws_region = st.secrets.get("AWS_REGION", None)

if use_uploader and uploaded_file is not None:
    try:
        df_raw = load_csv_from_buffer(uploaded_file)
        data_source = f"Uploaded: {uploaded_file.name}"
    except Exception as e:
        st.error(f"Failed to read uploaded file: {e}")
        st.stop()
else:
    # If S3 is configured, try S3 first (so deployed app reads S3 by default)
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
        except Exception as e:
            st.warning(f"S3 read failed ({e}); falling back to local file if available.")
            df_raw = None

    # If S3 didn't provide data, try local path
    if df_raw is None:
        try:
            if csv_path and Path(csv_path).exists():
                df_raw = load_csv_from_buffer(csv_path)
                data_source = f"Local path: {csv_path}"
        except Exception as e:
            st.error(f"Failed to read local file: {e}")
            st.stop()
    if df_raw is None:
        st.error("No transaction data found: upload a file, set local path, or configure S3 secrets (S3_BUCKET/S3_KEY).")
        st.stop()

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

# Categorization function
def categorize_counterparty(counterparty):
    """Categorize counterparty based on personal_categories config"""
    if pd.isna(counterparty):
        return "no-category"
    
    counterparty_str = str(counterparty).strip()
    
    # Direct match first
    if counterparty_str in personal_categories:
        return personal_categories[counterparty_str]
    
    # Special case for Lidl stores (dynamic matching)
    if 'lidl' in counterparty_str.lower():
        return 'Groceries'
    
    # Check for partial matches (case-insensitive)
    counterparty_lower = counterparty_str.lower()
    for key, category in personal_categories.items():
        if key.lower() in counterparty_lower:
            return category
    
    # Default category for uncategorized items
    return "no-category"

# Apply categorization
df["category"] = df["counterparty"].apply(categorize_counterparty)

# Reference dates
last_date_with_time = df["date"].max()  # Keep original time
last_date = df["date"].max().normalize()
today = pd.Timestamp.now().normalize()

# Compute month period that contains last_date
month_start = last_date.replace(day=1)
month_end = last_date  # up to last_date present in CSV

# Calendar week: Monday-based week that contains last_date
week_start = (last_date - pd.Timedelta(days=int(last_date.weekday()))).normalize()
week_end = last_date

# Calculate how many weeks have passed in the current month
# Week calculation: cumulative spending from month start vs accumulated weekly limits
weeks_in_month = ((last_date - month_start).days // 7) + 1  # How many weeks into the month we are
accumulated_weekly_limit = weekly_limit * weeks_in_month

# Totals - calculate net expenses (gross expenses minus refunds)
# Use same period logic as monthly tables for consistency
current_period = last_date.to_period("M")
period_mask = df["date"].dt.to_period("M") == current_period
current_month_df = df.loc[period_mask]

gross_month_expenses = current_month_df[current_month_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive

# Calculate refunds for current month (non-USD positive transactions)
current_month_positive = current_month_df[current_month_df["amount_dkk"] > 0]
current_month_refunds = current_month_positive[current_month_positive["currency"] != "USD"]["amount_dkk"].sum()

# Net spending = gross expenses - refunds
spent_month_to_last_date = gross_month_expenses - current_month_refunds

# For weekly: use cumulative monthly spending vs accumulated weekly limits
spent_week_cumulative = spent_month_to_last_date  # This is cumulative from month start

# Calculate current week spending only (for display purposes) - also net
current_week_df = df.loc[(df["date"] >= week_start) & (df["date"] <= week_end)]
gross_week_expenses = current_week_df[current_week_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive
current_week_positive = current_week_df[current_week_df["amount_dkk"] > 0]
current_week_refunds = current_week_positive[current_week_positive["currency"] != "USD"]["amount_dkk"].sum()
spent_current_week_only = gross_week_expenses - current_week_refunds

# Also show "till today" totals (if CSV contains entries up to today, it'll be same)
spent_month_to_today = df.loc[(df["date"] >= today.replace(day=1)) & (df["date"] <= today), "expense_dkk"].sum() if today <= last_date else spent_month_to_last_date
# For cumulative weekly calculation, use month-to-date spending vs today's accumulated limit
weeks_in_month_today = ((today - today.replace(day=1)).days // 7) + 1 if today <= last_date else weeks_in_month
accumulated_weekly_limit_today = weekly_limit * weeks_in_month_today
spent_week_to_today = spent_month_to_today  # Cumulative from month start

# Comparison computations
month_remaining = monthly_limit - spent_month_to_last_date
week_remaining = accumulated_weekly_limit - spent_week_cumulative

month_pct = min(max(spent_month_to_last_date / monthly_limit, 0), 2) if monthly_limit > 0 else 0
week_pct = min(max(spent_week_cumulative / accumulated_weekly_limit, 0), 2) if accumulated_weekly_limit > 0 else 0

# Layout: top metrics

# Format latest transaction with time if available
if last_date_with_time.time() != pd.Timestamp("00:00:00").time():
    # Time exists, show it
    st.write(f"Latest transaction: **{last_date_with_time.strftime('%B %d, %Y at %H:%M')}**")
else:
    # No time, show date only
    st.write(f"Latest transaction: **{last_date.strftime('%B %d, %Y')}**")
st.write(f"Today: **{today.date()}**")
st.markdown(f"----")
col1, col2 = st.columns([1.2, 1.2])
with col1:
    st.subheader(f"{last_date.strftime('%B %Y')}")
    st.write(f"All refunds for this month is subtracted from expenses.")
    label = f"{month_start.strftime('%Y-%m-%d')} → {month_end.strftime('%Y-%m-%d')}"
    
    # Determine color and delta text based on spending vs limit
    monthly_usage_pct = spent_month_to_last_date / monthly_limit if monthly_limit > 0 else 0
    
    if monthly_usage_pct >= 1.0:  # Over limit
        delta_color = "inverse"  # Red
        progress_color = "red"
        delta = f"{abs(month_remaining):,.0f} DKK over"  # Use positive number with "inverse" to show red
    elif monthly_usage_pct >= 0.9:  # Within 10% of limit
        delta_color = "off"  # Orange/neutral
        progress_color = "orange"
        delta = f"{month_remaining:,.0f} DKK remaining"
    else:  # Safe zone
        delta_color = "normal"  # Green
        progress_color = "green"
        delta = f"{month_remaining:,.0f} DKK remaining"
    
    st.metric(label="Spent", value=f"{spent_month_to_last_date:,.0f} DKK", delta=delta, delta_color=delta_color)
    
    # Colored progress bar
    progress_value = min(monthly_usage_pct, 1.0)
    if progress_color == "red":
        st.markdown(f'<div style="background: linear-gradient(90deg, #ff4b4b 0%, #ff4b4b {progress_value*100:.1f}%, #333 {progress_value*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)
    elif progress_color == "orange":
        st.markdown(f'<div style="background: linear-gradient(90deg, #ffaa00 0%, #ffaa00 {progress_value*100:.1f}%, #333 {progress_value*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="background: linear-gradient(90deg, #00cc88 0%, #00cc88 {progress_value*100:.1f}%, #333 {progress_value*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)

with col2:
    st.subheader(f"Cumulative weekly")
    labelw = f"{month_start.strftime('%Y-%m-%d')} → {week_end.strftime('%Y-%m-%d')} (Week {weeks_in_month})"
    
    # Determine color and delta text based on cumulative weekly spending vs accumulated limit
    weekly_usage_pct = spent_week_cumulative / accumulated_weekly_limit if accumulated_weekly_limit > 0 else 0
    
    if weekly_usage_pct >= 1.0:  # Over limit
        delta_color_weekly = "inverse"  # Red
        progress_color_weekly = "red"
        deltaw = f"{abs(week_remaining):,.0f} DKK over"  # Use positive number with "inverse" to show red
    elif weekly_usage_pct >= 0.9:  # Within 10% of limit
        delta_color_weekly = "off"  # Orange/neutral
        progress_color_weekly = "orange"
        deltaw = f"{week_remaining:,.0f} DKK remaining"
    else:  # Safe zone
        delta_color_weekly = "normal"  # Green
        progress_color_weekly = "green"
        deltaw = f"{week_remaining:,.0f} DKK remaining"
    
    st.metric(label="Spent", value=f"{spent_current_week_only:,.0f} DKK", delta=deltaw, delta_color=delta_color_weekly)
    
    # Colored progress bar for weekly
    progress_value_weekly = min(weekly_usage_pct, 1.0)
    if progress_color_weekly == "red":
        st.markdown(f'<div style="background: linear-gradient(90deg, #ff4b4b 0%, #ff4b4b {progress_value_weekly*100:.1f}%, #333 {progress_value_weekly*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)
    elif progress_color_weekly == "orange":
        st.markdown(f'<div style="background: linear-gradient(90deg, #ffaa00 0%, #ffaa00 {progress_value_weekly*100:.1f}%, #333 {progress_value_weekly*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="background: linear-gradient(90deg, #00cc88 0%, #00cc88 {progress_value_weekly*100:.1f}%, #333 {progress_value_weekly*100:.1f}%, #333 100%); height: 8px; border-radius: 4px;"></div>', unsafe_allow_html=True)
    
    st.caption(f"Cumulative: {spent_week_cumulative:,.0f} DKK / {accumulated_weekly_limit:,.0f} DKK ({weeks_in_month} weeks × {weekly_limit:,.0f} DKK)")

st.markdown("---")

# Daily cumulative chart for current month (up to last_date)
month_days = pd.date_range(start=month_start, end=month_end, freq='D')
daily = df.loc[(df["date"] >= month_start) & (df["date"] <= month_end)].copy()
daily_sum = daily.groupby(daily["date"].dt.normalize())["expense_dkk"].sum().reindex(month_days, fill_value=0).cumsum()
daily_df = pd.DataFrame({"date": month_days, "cumulative_spent": daily_sum.values})
daily_df["limit_progress"] = [(i+1)/len(month_days) * monthly_limit for i in range(len(month_days))]

# Create responsive chart with better mobile layout
fig2 = px.line(daily_df, x="date", y="cumulative_spent", 
               title=f"Cumulative spending (through {last_date.strftime('%b %d')})", 
               labels={"cumulative_spent":"Spent (DKK)", "date": ""})
fig2.add_scatter(x=daily_df["date"], y=daily_df["limit_progress"], mode="lines", 
                name="Pro-rated limit", line=dict(dash="dash", color="#FFA500"))

# Responsive layout settings - disable interactivity for mobile
fig2.update_layout(
    height=300,  # Slightly shorter for mobile
    margin=dict(t=50, l=20, r=20, b=20),  # Tighter margins
    title=dict(x=0.5, font=dict(size=14)),  # Center title with smaller font
    legend=dict(
        orientation="h",  # Horizontal legend
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)  # Smaller legend font
    ),
    xaxis=dict(
        title="",  # Remove x-axis title to save space
        tickfont=dict(size=10),
        tickangle=0  # Keep dates horizontal
    ),
    yaxis=dict(
        title="DKK",  # Short y-axis title
        title_font=dict(size=12),
        tickfont=dict(size=10)
    ),
    # Disable interactivity for mobile-friendly experience
    dragmode=False
)

# Disable hover and interactions
fig2.update_traces(hoverinfo='skip', hovertemplate=None)

st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

st.header("Top Expenses")

# Build the 4 monthly periods (reference = last_date from CSV)
last_period = last_date.to_period("M")
periods = [last_period - i for i in range(0, 4)]  # last_period, last-1, last-2, last-3
# Show newest -> oldest (left -> right): Oct, Sep, Aug, Jul
# periods is already in the correct order: [Oct, Sep, Aug, Jul]

cols = st.columns(4)
for i, period in enumerate(periods):
    col = cols[i]
    # Filter rows for this period
    mask = df["date"].dt.to_period("M") == period
    month_df = df.loc[mask].copy()
    display_label = period.strftime("%b %Y")  # e.g. "Oct 2025"
    with col:
        # Calculate totals for this month in DKK
        gross_expense_dkk = month_df[month_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive
        
        # Separate income (USD positive) from refunds (other positive currencies)
        positive_df = month_df[month_df["amount_dkk"] > 0]
        
        # Calculate USD income (both DKK converted and original USD amounts)
        usd_transactions = positive_df[positive_df["currency"] == "USD"]
        actual_income_dkk = usd_transactions["amount_dkk"].sum()
        actual_income_usd = usd_transactions["amount"].sum()  # Original USD amount
        
        refund_dkk = positive_df[positive_df["currency"] != "USD"]["amount_dkk"].sum()
        
        # Calculate net expenses (gross expenses minus refunds for this month)
        net_expense_dkk = gross_expense_dkk - refund_dkk
        
        # Count total items (unique counterparties)
        total_counterparties = month_df["counterparty"].nunique()
        
        st.subheader(display_label)
        if month_df.empty:
            st.write("No data")
            continue
        
        # Display totals and item count with emojis only (compact for mobile)
        # Show original USD amount in brackets for income
        if actual_income_usd > 0:
            income_display = f"💰 {actual_income_dkk:,.0f} DKK [${actual_income_usd:,.0f}]"
        else:
            income_display = f"💰 {actual_income_dkk:,.0f} DKK"
            
        st.caption(f"💸 {net_expense_dkk:,.0f} DKK | {income_display} | ♻️ {refund_dkk:,.0f} DKK | 📊 {total_counterparties}")

        # Show individual expense transactions (remove income from table)
        expense_transactions = month_df[month_df["amount_dkk"] < 0].copy()
        expense_transactions = expense_transactions.sort_values("amount_dkk")  # Sort by amount (most negative first)
        
        # Create display data with individual transactions
        display_data = []
        
        # Add individual expense transactions (show as positive amounts)
        for _, transaction in expense_transactions.iterrows():
            amount_dkk = abs(transaction["amount_dkk"])
            counterparty = transaction["counterparty"]
            category = transaction["category"]
            # Format as MM-DD HH:MM
            datetime_str = transaction["date"].strftime("%m-%d %H:%M")
            
            display_data.append({
                "Counterparty": counterparty, 
                "Amount (DKK)": int(amount_dkk), 
                "Datetime": datetime_str,
                "Category": category
            })
        
        if display_data:
            table_df = pd.DataFrame(display_data)
            # Show scrollable table with max 8 visible rows
            st.dataframe(
                table_df, 
                height=320,  # Fixed height to show ~8 rows with scroll
                use_container_width=True,
                hide_index=True
            )
        else:
            st.write("No transactions")
            continue

        # Mini horizontal bar chart by categories (show top expense categories only)
        try:
            if not month_df.empty:
                # Group expenses by category for the current month
                expense_month_df = month_df[month_df["amount_dkk"] < 0].copy()
                if not expense_month_df.empty:
                    category_expenses = (
                        expense_month_df.groupby("category")["amount_dkk"]
                        .sum()
                        .abs()  # Convert to positive
                        .sort_values(ascending=False)
                        .head(8)  # Top 8 categories
                    )
                    
                    if not category_expenses.empty:
                        category_plot_df = pd.DataFrame({
                            "Category": category_expenses.index,
                            "Amount (DKK)": category_expenses.values.astype(int)
                        })
                        
                        fig_small = px.bar(
                            category_plot_df,
                            x="Amount (DKK)",
                            y="Category",
                            orientation="h",
                            text="Amount (DKK)",
                            color_discrete_sequence=["#D9534F"],  # Red for expenses
                        )
                        fig_small.update_traces(
                            texttemplate="%{text:,}", 
                            textposition="inside",
                            textfont=dict(size=12)  # Bigger text for mobile
                        )
                        fig_small.update_layout(
                            height=220,
                            margin=dict(t=10, l=10, r=10, b=10),
                            xaxis_title="DKK",
                            yaxis=dict(
                                autorange="reversed",  # keep largest on top
                                title=""  # Remove y-axis label
                            ),
                            showlegend=False,
                            # Disable interactivity for mobile
                            dragmode=False
                        )
                        # Disable hover and interactions
                        fig_small.update_traces(hoverinfo='skip', hovertemplate=None)
                        st.plotly_chart(fig_small, use_container_width=True)
        except Exception:
            # If plotting fails, skip gracefully
            pass

# End of file