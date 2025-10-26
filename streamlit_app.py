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
import pytz

# Import personal categories and monthly limits for expense categorization
try:
    from config import personal_categories, get_monthly_limit
    config_loaded = True
    # st.sidebar.success(f"✅ Config loaded: {len(personal_categories)} categories")
except ImportError as e:
    # Fallback categories for when config.py is not available (e.g., in deployment)
    personal_categories = {
        # Essential categories for basic functionality
        'Netto': 'Groceries', 'Kvickly': 'Groceries', 'nemlig.com': 'Groceries',
        'Coop 365': 'Groceries', 'REMA 1000': 'Groceries', 'føtex': 'Groceries',
        'Circle K': 'Fuel', 'Shell': 'Fuel', 'Q8': 'Fuel', 'Uno-X': 'Fuel',
        'Amazon': 'Amazon', 'Amazon Web Services': 'Amazon',
        'Boozt.com': 'Clothes', 'Mango': 'Clothes', 'Adidas': 'Clothes',
        'McDonald\'s': 'Eat Out', 'Lagkagehuset': 'Eat Out', '7-Eleven': 'Eat Out',
        'Roedovre Skoejte Isho': 'Ice Hockey', 'Holdsport': 'Ice Hockey',
        'IKEA': 'Home Maintenance', 'Bauhaus': 'Home Maintenance',
    }
    config_loaded = False
    st.sidebar.warning(f"⚠️ Using fallback config: {len(personal_categories)} basic categories")
    
    # Monthly limits fallback
    def get_monthly_limit(year, month):
        # Basic fallback limits
        if month == 12 or month == 1:  # December or January
            return 21000
        return 18000

st.set_page_config(page_title=" Budget Tracker", layout="wide")
st.markdown("<h1 style='margin:0 0 8px 0'>🏠 Budget Tracker</h1>", unsafe_allow_html=True)

st.sidebar.header("Data / Limits")
default_path = Path("data/transaction-history.csv")
csv_path = st.sidebar.text_input("Local CSV path (used if not uploading)", value=str(default_path))

# Monthly limit - use config as default but allow user adjustment
default_monthly_limit = 18000  # Will be updated after we load the config
monthly_limit_input = st.sidebar.number_input("Monthly limit (DKK)", min_value=0, value=default_monthly_limit, step=500, format="%d", help="Adjustable for testing - resets to config.py value on refresh")

# Calculate weekly limit from monthly
weekly_default = float(monthly_limit_input) / 4.33
weekly_limit = st.sidebar.number_input("Weekly limit (DKK)", min_value=0.0, value=round(weekly_default, 2), step=100.0, format="%.2f")

st.sidebar.markdown("---")
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

def load_csv_from_s3(bucket: str, key: str, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, profile_name=None):
    try:
        if aws_access_key_id and aws_secret_access_key:
            # Use explicit credentials
            session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
        elif profile_name:
            # Use specific profile
            session = boto3.session.Session(
                profile_name=profile_name,
                region_name=region_name,
            )
        else:
            # Use default credentials but with explicit region
            session = boto3.session.Session(region_name=region_name)
        
        s3 = session.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return pd.read_csv(io.BytesIO(body))
    except Exception as e:
        # Re-raise with more context
        raise Exception(f"S3 read failed: {str(e)}")

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
            # Use profile if no explicit credentials are provided
            profile_name = "streamlit-reader" if not (aws_id and aws_secret) else None
            
            df_raw = load_csv_from_s3(
                bucket=s3_bucket,
                key=s3_key,
                aws_access_key_id=aws_id,
                aws_secret_access_key=aws_secret,
                region_name=aws_region,
                profile_name=profile_name,
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
# NEW: status candidates
status_candidates = ["Status", "status", "STATUS", "State", "state", "Transaction Status"]

date_col = next((c for c in date_candidates if c in df_raw.columns), None)
amt_col = next((c for c in amt_candidates if c in df_raw.columns), None)
cur_col = next((c for c in cur_candidates if c in df_raw.columns), None)
dir_col = next((c for c in dir_candidates if c in df_raw.columns), None)
cp_col = next((c for c in cp_candidates if c in df_raw.columns), None)
status_col = next((c for c in status_candidates if c in df_raw.columns), None)

if date_col is None or amt_col is None:
    st.error("Could not find required columns (date and amount). Please ensure CSV has a date and an amount column.")
    st.stop()

# NEW: drop cancelled rows before any parsing/calculation
if status_col is not None:
    status_upper = df_raw[status_col].astype(str).str.upper().str.strip()
    keep_mask = ~status_upper.isin(["CANCELLED", "CANCELED"])
    cancelled_count = int((~keep_mask).sum())
    if cancelled_count > 0:
        st.sidebar.info(f"⏭️ Skipped {cancelled_count} cancelled transactions")
    df_raw = df_raw[keep_mask].copy()

# Parse date column
df_raw[date_col] = pd.to_datetime(df_raw[date_col], errors="coerce")

# Set up Denmark timezone
denmark_tz = pytz.timezone('Europe/Copenhagen')

# Convert dates to Denmark timezone if they have timezone info, or localize if naive
if df_raw[date_col].dt.tz is not None:
    # Dates have timezone info, convert to Denmark time
    df_raw[date_col] = df_raw[date_col].dt.tz_convert(denmark_tz)
else:
    # Dates are naive (no timezone), assume UTC and convert to Denmark time
    df_raw[date_col] = df_raw[date_col].dt.tz_localize('UTC').dt.tz_convert(denmark_tz)
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

# Debug info for categorization
if not config_loaded:
    st.sidebar.warning("⚠️ Using fallback categorization")
    st.sidebar.caption("To fix: Ensure config.py is deployed and accessible")
else:
    unique_categories = df["category"].value_counts()
    no_category_count = unique_categories.get("no-category", 0)
    total_transactions = len(df)
    categorized_pct = ((total_transactions - no_category_count) / total_transactions * 100) if total_transactions > 0 else 0
    st.sidebar.info(f"📊 Categorized: {categorized_pct:.1f}% of transactions")

# Show category breakdown in sidebar for debugging
with st.sidebar.expander("🏷️ Category Breakdown"):
    category_counts = df["category"].value_counts().head(10)
    for category, count in category_counts.items():
        st.write(f"**{category}**: {count} transactions")

# Reference dates
last_date_with_time = df["date"].max()  # Keep original time (now in Denmark timezone)
last_date = df["date"].max().normalize()
# Get current time in Denmark timezone
today = pd.Timestamp.now(tz=denmark_tz).normalize()

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

# Budget carry-over calculation (starting from October 2025)
def calculate_budget_carryover(df, current_date):
    """Calculate budget surplus/deficit carry-over from October 2025 onwards"""
    carry_over_start = pd.Timestamp('2025-10-01', tz=denmark_tz)
    current_period = current_date.to_period('M')
    
    if current_date < carry_over_start:
        return 0, []  # No carry-over before October 2025
    
    carry_over_amount = 0
    carry_over_details = []
    
    # Calculate carry-over from October 2025 to current month (exclusive)
    start_period = carry_over_start.to_period('M')  # 2025-10
    
    for period in pd.period_range(start_period, current_period, freq='M'):
        if period >= current_period:
            break  # Don't include current month
            
        # Get spending for this period
        period_mask = df["date"].dt.to_period("M") == period
        period_df = df.loc[period_mask]
        
        if period_df.empty:
            continue  # Skip months with no data
            
        # Calculate net spending for this month (same logic as main calculation)
        gross_expenses = period_df[period_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1
        period_positive = period_df[period_df["amount_dkk"] > 0]
        
        # Exclude cashback from refunds
        cashback_mask = (
            period_positive.get("ID", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
            period_positive.get("Reference", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
            period_positive["counterparty"].str.contains("CASHBACK", case=False, na=False)
        )
        period_refunds = period_positive[
            (period_positive["currency"] != "USD") & (~cashback_mask)
        ]["amount_dkk"].sum()
        
        net_spending = gross_expenses - period_refunds
        
        # Get the budget limit for this month
        year = period.year
        month = period.month
        budget_limit = get_monthly_limit(year, month)
        
        # Calculate surplus (negative) or deficit (positive)
        month_result = net_spending - budget_limit
        carry_over_amount += month_result
        
        carry_over_details.append({
            'period': period,
            'spending': net_spending,
            'limit': budget_limit,
            'result': month_result
        })
    
    return carry_over_amount, carry_over_details

# Calculate budget carry-over and adjusted monthly limit
carry_over_amount, carry_over_details = calculate_budget_carryover(df, last_date)

# Get base monthly limit for current month from config
current_year = last_date.year
current_month = last_date.month
base_monthly_limit = get_monthly_limit(current_year, current_month)

# Use user input for monthly limit, or fall back to config value
# Adjusted monthly limit = user input - carry over (surplus adds to limit, deficit reduces it)
monthly_limit = monthly_limit_input - carry_over_amount

# Update sidebar with current month's budget details
st.sidebar.markdown("---")
st.sidebar.subheader("Current Month Budget")
st.sidebar.write(f"**{last_date.strftime('%B %Y')}**")
st.sidebar.write(f"Config default: {base_monthly_limit:,.0f} DKK")
st.sidebar.write(f"Your setting: {monthly_limit_input:,.0f} DKK")

if carry_over_amount != 0:
    if carry_over_amount > 0:  # Deficit
        st.sidebar.write(f"❌ Carry-over deficit: -{carry_over_amount:,.0f} DKK")
        st.sidebar.write(f"🎯 **Adjusted limit: {monthly_limit:,.0f} DKK**")
    else:  # Surplus
        st.sidebar.write(f"✅ Carry-over surplus: +{abs(carry_over_amount):,.0f} DKK") 
        st.sidebar.write(f"🎯 **Adjusted limit: {monthly_limit:,.0f} DKK**")
else:
    st.sidebar.write("ℹ️ No carry-over (first month or no data)")
    st.sidebar.write(f"🎯 **Current limit: {monthly_limit:,.0f} DKK**")

# Show carry-over details if available
if len(carry_over_details) > 0:
    with st.sidebar.expander("📊 Carry-over Details"):
        for detail in carry_over_details[-3:]:  # Show last 3 months
            period_str = detail['period'].strftime('%b %Y')
            spending = detail['spending']
            limit = detail['limit']
            result = detail['result']
            if result > 0:
                st.write(f"**{period_str}**: {spending:,.0f}/{limit:,.0f} DKK (❌ +{result:,.0f})")
            else:
                st.write(f"**{period_str}**: {spending:,.0f}/{limit:,.0f} DKK (✅ {result:,.0f})")
        if len(carry_over_details) > 3:
            st.write(f"... and {len(carry_over_details)-3} earlier months")

# Totals - calculate net expenses (gross expenses minus refunds)
# Use same period logic as monthly tables for consistency
current_period = last_date.to_period("M")
period_mask = df["date"].dt.to_period("M") == current_period
current_month_df = df.loc[period_mask]

gross_month_expenses = current_month_df[current_month_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive

# Calculate refunds for current month (non-USD positive transactions, excluding cashback)
current_month_positive = current_month_df[current_month_df["amount_dkk"] > 0]
# Exclude cashback from refunds (cashback should be treated as income)
# Check for cashback in multiple columns (ID, Reference, counterparty)
cashback_mask = (
    current_month_positive.get("ID", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
    current_month_positive.get("Reference", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
    current_month_positive["counterparty"].str.contains("CASHBACK", case=False, na=False)
)
current_month_refunds_df = current_month_positive[
    (current_month_positive["currency"] != "USD") & (~cashback_mask)
]
current_month_refunds = current_month_refunds_df["amount_dkk"].sum()

# Net spending = gross expenses - refunds
spent_month_to_last_date = gross_month_expenses - current_month_refunds

# For weekly: use cumulative monthly spending vs accumulated weekly limits
spent_week_cumulative = spent_month_to_last_date  # This is cumulative from month start

# Calculate current week spending only (for display purposes) - also net
current_week_df = df.loc[(df["date"] >= week_start) & (df["date"] <= week_end)]
gross_week_expenses = current_week_df[current_week_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive
current_week_positive = current_week_df[current_week_df["amount_dkk"] > 0]
# Exclude cashback from refunds (cashback should be treated as income)
# Check for cashback in multiple columns (ID, Reference, counterparty)
cashback_mask_week = (
    current_week_positive.get("ID", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
    current_week_positive.get("Reference", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
    current_week_positive["counterparty"].str.contains("CASHBACK", case=False, na=False)
)
current_week_refunds_df = current_week_positive[
    (current_week_positive["currency"] != "USD") & (~cashback_mask_week)
]
current_week_refunds = current_week_refunds_df["amount_dkk"].sum()
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
if last_date_with_time.time() != pd.Timestamp("00:00:00", tz=denmark_tz).time():
    # Time exists, show it in Denmark timezone
    st.write(f"Latest transaction: **{last_date_with_time.strftime('%B %d, %Y at %H:%M')} (DK time)**")
else:
    # No time, show date only
    st.write(f"Latest transaction: **{last_date.strftime('%B %d, %Y')} (DK time)**")
st.write(f"Today: **{today.date()} (DK time)**")
st.markdown(f"----")
col1, col2 = st.columns([1.2, 1.2])
with col1:
    st.subheader(f"{last_date.strftime('%B %Y')}")
    st.write(f"All refunds for this month are subtracted from expenses. Cashback is treated as income.")
    
    # Show budget information with carry-over details
    if carry_over_amount != 0:
        if carry_over_amount > 0:  # Deficit from previous months
            carry_over_text = f"⚠️ Deficit from prev months: -{carry_over_amount:,.0f} DKK"
        else:  # Surplus from previous months
            carry_over_text = f"✅ Surplus from prev months: +{abs(carry_over_amount):,.0f} DKK"
        st.caption(f"Base limit: {base_monthly_limit:,.0f} DKK | {carry_over_text}")
    else:
        st.caption(f"Monthly limit: {base_monthly_limit:,.0f} DKK (no carry-over)")
    
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
    st.markdown("##### Cumulative weekly")
    st.write("Spending cumulates from month start vs weekly budget.")
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

# Daily cumulative chart for current month (full month from 1st to last day)
# Get the full month range from 1st to last day of the month
month_first_day = last_date.replace(day=1)
month_last_day = (month_first_day + pd.DateOffset(months=1) - pd.DateOffset(days=1)).normalize()
# Create timezone-aware date range for Denmark
month_days = pd.date_range(start=month_first_day, end=month_last_day, freq='D', tz=denmark_tz)

daily = df.loc[(df["date"] >= month_first_day) & (df["date"] <= month_last_day)].copy()

# Calculate daily net spending (gross expenses minus refunds, same logic as top metrics)
daily_net_spending = []
for day in month_days:
    day_df = daily[daily["date"].dt.normalize() == day.normalize()]
    
    if day_df.empty:
        daily_net_spending.append(0)
        continue
    
    # Gross expenses for this day
    day_gross_expenses = day_df[day_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive
    
    # Refunds for this day (same logic as monthly calculation)
    day_positive = day_df[day_df["amount_dkk"] > 0]
    cashback_mask_day = (
        day_positive.get("ID", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
        day_positive.get("Reference", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
        day_positive["counterparty"].str.contains("CASHBACK", case=False, na=False)
    )
    day_refunds = day_positive[
        (day_positive["currency"] != "USD") & (~cashback_mask_day)
    ]["amount_dkk"].sum()
    
    # Net spending = gross expenses - refunds
    day_net = day_gross_expenses - day_refunds
    daily_net_spending.append(day_net)

daily_sum = pd.Series(daily_net_spending, index=month_days).cumsum()

# Create separate dataframes for spending (only up to last_date) and limit (full month)
spending_days = pd.date_range(start=month_first_day, end=last_date, freq='D', tz=denmark_tz)
spending_sum = daily_sum.reindex(spending_days)

# If last_date is before today, extend the spending line to today with the same value
if last_date < today:
    # Get the last spending value
    last_spending_value = spending_sum.iloc[-1] if not spending_sum.empty else 0
    
    # Create extended date range from last_date + 1 day to today
    extended_days = pd.date_range(start=last_date + pd.Timedelta(days=1), end=today, freq='D', tz=denmark_tz)
    
    # Create extended spending series with constant last value
    extended_spending = pd.Series([last_spending_value] * len(extended_days), index=extended_days)
    
    # Combine original and extended spending
    all_spending_days = pd.date_range(start=month_first_day, end=today, freq='D', tz=denmark_tz)
    combined_spending = pd.concat([spending_sum, extended_spending])
else:
    # If last_date is today or later, use original data
    all_spending_days = spending_days
    combined_spending = spending_sum

spending_df = pd.DataFrame({"date": all_spending_days, "cumulative_spent": combined_spending.values})

# Extend limit line to today as well
limit_days_extended = pd.date_range(start=month_first_day, end=max(today, month_last_day), freq='D', tz=denmark_tz)
limit_df = pd.DataFrame({"date": limit_days_extended, "limit_progress": [(i+1)/len(month_days) * monthly_limit for i in range(len(limit_days_extended))]})

# Create responsive chart with conditional coloring
fig2 = go.Figure()

# Add pro-rated limit line first (so it appears behind)
fig2.add_trace(go.Scatter(
    x=limit_df["date"], 
    y=limit_df["limit_progress"], 
    mode="lines",
    line=dict(dash="dash", color="#FFA500", width=2),
    name="Pro-rated limit",
    showlegend=False
))

# Calculate corresponding limit values for spending dates to determine color
if not spending_df.empty:
    spending_with_limit = spending_df.copy()
    spending_with_limit["days_from_start"] = (spending_with_limit["date"] - month_first_day).dt.days + 1
    spending_with_limit["corresponding_limit"] = spending_with_limit["days_from_start"] / len(month_days) * monthly_limit
    
    # Find the index where CSV data ends (last_date) to change line style
    csv_end_index = None
    if last_date < today:
        csv_end_index = len(pd.date_range(start=month_first_day, end=last_date, freq='D', tz=denmark_tz)) - 1
    
    # Create segments based on whether spending is above or below limit
    for i in range(len(spending_with_limit)):
        current_row = spending_with_limit.iloc[i]
        
        # Determine color based on spending vs limit
        is_over_limit = current_row["cumulative_spent"] > current_row["corresponding_limit"]
        color = "#ff4b4b" if is_over_limit else "#1f77b4"  # Red if over, blue if under
        
        # Determine if this is extended data (after CSV ends)
        is_extended = csv_end_index is not None and i > csv_end_index
        line_style = dict(color=color, width=3, dash="dot" if is_extended else "solid")
        
        # Add line segment from previous point to current point
        if i > 0:
            prev_row = spending_with_limit.iloc[i-1]
            fig2.add_trace(go.Scatter(
                x=[prev_row["date"], current_row["date"]], 
                y=[prev_row["cumulative_spent"], current_row["cumulative_spent"]],
                mode="lines",
                line=line_style,
                showlegend=False,
                hoverinfo='skip'
            ))
        elif i == 0:
            # First point - add as a single point line
            fig2.add_trace(go.Scatter(
                x=[current_row["date"]], 
                y=[current_row["cumulative_spent"]],
                mode="lines",
                line=line_style,
                showlegend=False,
                hoverinfo='skip'
            ))

    # Add different colored dots for CSV end and current position
    if csv_end_index is not None and csv_end_index < len(spending_df):
        # Green dot at CSV data end (last transaction date)
        csv_end_point = spending_df.iloc[csv_end_index]
        fig2.add_trace(go.Scatter(
            x=[csv_end_point["date"]], 
            y=[csv_end_point["cumulative_spent"]], 
            mode="markers", 
            marker=dict(color="green", size=8),
            name=f"Last transaction ({last_date.strftime('%b %d')})",
            showlegend=False,
            hovertemplate=f"Last transaction: {last_date.strftime('%B %d')}<br>Amount: {csv_end_point['cumulative_spent']:,.0f} DKK<extra></extra>"
        ))
    
    # Orange dot at today's position (end of extended line)
    if last_date < today and not spending_df.empty:
        today_point = spending_df.iloc[-1]
        fig2.add_trace(go.Scatter(
            x=[today_point["date"]], 
            y=[today_point["cumulative_spent"]], 
            mode="markers", 
            marker=dict(color="orange", size=8),
            name=f"Today ({today.strftime('%b %d')})",
            showlegend=False,
            hovertemplate=f"Today: {today.strftime('%B %d')}<br>Amount: {today_point['cumulative_spent']:,.0f} DKK<br>(No new transactions)<extra></extra>"
        ))
    elif not spending_df.empty:
        # If CSV is up to date, show green dot at current position
        current_point = spending_df.iloc[-1]
        fig2.add_trace(go.Scatter(
            x=[current_point["date"]], 
            y=[current_point["cumulative_spent"]], 
            mode="markers", 
            marker=dict(color="green", size=8),
            name="Current position",
            showlegend=False,
            hoverinfo='skip'
        ))

# Responsive layout settings - disable interactivity for mobile
fig2.update_layout(
    height=300,  # Slightly shorter for mobile
    margin=dict(t=50, l=20, r=20, b=20),  # Tighter margins
    title=dict(text="Cumulative spending (DKK)", x=0.5, font=dict(size=14)),  # Center title with smaller font
    showlegend=False,  # Remove legend
    xaxis=dict(
        title="",  # Remove x-axis title to save space
        tickfont=dict(size=10),
        tickangle=0  # Keep dates horizontal
    ),
    yaxis=dict(
        title="",  # Remove y-axis title
        tickfont=dict(size=10)
    ),
    # Disable interactivity for mobile-friendly experience
    dragmode=False
)

st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

st.header("Top Expenses")

# Build the 8 monthly periods (reference = last_date from CSV)
last_period = last_date.to_period("M")
periods = [last_period - i for i in range(0, 8)]  # last_period, last-1, last-2, ..., last-7
# Show newest -> oldest (left -> right): Oct, Sep, Aug, Jul, Jun, May, Apr, Mar
# periods is already in the correct order

# Split into rows of 3 columns each (3+3+2 for 8 months)
for row in range(3):
    if row > 0:  # Add separator between rows
        st.markdown("---")
    
    # Determine columns for this row
    if row < 2:  # First two rows have 3 columns
        cols = st.columns(3)
        cols_in_row = 3
    else:  # Last row has 2 columns
        cols = st.columns(2)
        cols_in_row = 2
    
    for col_idx in range(cols_in_row):
        period_idx = row * 3 + col_idx
        if period_idx >= len(periods):
            break
        
        period = periods[period_idx]
        col = cols[col_idx]
        
        # Filter rows for this period
        mask = df["date"].dt.to_period("M") == period
        month_df = df.loc[mask].copy()
        display_label = period.strftime("%b %Y")  # e.g. "Oct 2025"
        with col:
            # Calculate totals for this month in DKK
            gross_expense_dkk = month_df[month_df["amount_dkk"] < 0]["amount_dkk"].sum() * -1  # Convert to positive
            
            # Separate income (USD positive + Cashback) from refunds (other positive currencies)
            positive_df = month_df[month_df["amount_dkk"] > 0]
            
            # Calculate USD income (both DKK converted and original USD amounts)
            usd_transactions = positive_df[positive_df["currency"] == "USD"]
            actual_income_usd = usd_transactions["amount"].sum()  # Original USD amount
            
            # Calculate cashback income (any currency)
            # Check for cashbook in multiple columns (ID, Reference, counterparty)
            cashback_mask_display = (
                positive_df.get("ID", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
                positive_df.get("Reference", pd.Series(dtype=str)).str.contains("CASHBACK", case=False, na=False) |
                positive_df["counterparty"].str.contains("CASHBACK", case=False, na=False)
            )
            cashback_transactions = positive_df[cashback_mask_display]
            cashback_income_dkk = cashback_transactions["amount_dkk"].sum()
            
            # Total income = USD transactions + Cashback
            actual_income_dkk = usd_transactions["amount_dkk"].sum() + cashback_income_dkk
            
            # Refunds = positive transactions excluding USD and Cashback
            refund_dkk = positive_df[
                (positive_df["currency"] != "USD") & (~cashback_mask_display)
            ]["amount_dkk"].sum()
            
            # Calculate net expenses (gross expenses minus refunds for this month)
            net_expense_dkk = gross_expense_dkk - refund_dkk
            
            # Count total items (unique counterparties)
            total_counterparties = month_df["counterparty"].nunique()
            
            st.subheader(display_label)
            if month_df.empty:
                st.write("No data")
                continue
            
            # Display totals and item count with emojis only (compact for mobile)
            # Show breakdown of income sources (USD salary + cashback)
            income_parts = []
            if actual_income_usd > 0:
                income_parts.append(f"${actual_income_usd:,.0f}")
            if cashback_income_dkk > 0:
                income_parts.append(f"{cashback_income_dkk:,.0f} DKK cashback")
            
            if income_parts:
                income_breakdown = " + ".join(income_parts)
                income_display = f"💰 {actual_income_dkk:,.0f} DKK [{income_breakdown}]"
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
                    height=340,  # Fixed height to show ~8 rows with scroll
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
                            .head(18)  # Top 18 categories
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
                                textfont=dict(size=20, color="white", family="Arial Black")
                            )
                            fig_small.update_layout(
                                height=260,
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