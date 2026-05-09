import sys
sys.path.insert(0, '.')
from processing import find_latest_account_statement_csv, load_revolut_csv, normalize_revolut_df, successful_transaction_mask
import pandas as pd

csv_path = find_latest_account_statement_csv('/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents')
print("CSV:", csv_path)
raw = load_revolut_csv(csv_path)
df = normalize_revolut_df(raw)

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
ex["to_currency"] = ex["description"].astype(str).str.extract(r"Exchanged to\s+([A-Za-z]{3})", expand=False).str.upper()
ex = ex[ex["to_currency"].isin(["USD", "GBP"])].copy()
ex["dkk_out"] = pd.to_numeric(ex["amount_net"], errors="coerce").abs()
print(f"Total rows: {len(ex)}")
print(f"Total DKK exchanged: {ex['dkk_out'].sum():,.2f}")
print(ex[["completed_date", "description", "to_currency", "dkk_out"]].to_string())
