# file: make_length_features.py
import pandas as pd
import numpy as np
import os

# ====================== INPUT ======================
base_dir  = r"C:\Users\Multiplexon\Desktop\data\d6"
file_name = "combine 2025_processed_filtered.csv"
file_path = os.path.join(base_dir, file_name)

df = pd.read_csv(file_path)

# ====================== HELPERS ======================
def hex_to_length(x):
    try:
        s = str(x).strip()
        if s.lower().startswith("0x"):
            return len(s[2:]) // 2
    except Exception:
        pass
    return None

def hex_to_int_maybe(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    try:
        if s.lower().startswith("0x"):
            return int(s, 16)
        return pd.to_numeric(s)
    except Exception:
        return np.nan

# ====================== HEX -> *_len ======================
hex_cols = ["Transaction Hash", "Original", "signature", "From", "To", "sender", "paymaster"]
for col in hex_cols:
    if col in df.columns:
        df[col + "_len"] = df[col].apply(hex_to_length)

# ====================== DateTime -> timestamp ======================
if "DateTime (UTC)" in df.columns:
    dt = pd.to_datetime(df["DateTime (UTC)"], errors="coerce", utc=True)
    df["DateTime_ts"] = (dt.view("int64") // 10**9)

# ====================== Numeric features ======================
numeric_targets = [
    "Txn Fee", "Gas Used", "logIndex", "actualGasCost",
    "actualGasUsed", "nonce", "success", "Blockno", "DateTime_ts"
]
for col in numeric_targets:
    if col in df.columns:
        df[col] = df[col].apply(hex_to_int_maybe)

# ====================== OUTPUT ======================
wanted_hex_len = [f"{c}_len" for c in hex_cols if f"{c}_len" in df.columns]
wanted_numeric = [c for c in numeric_targets if c in df.columns]
selected_cols = list(dict.fromkeys(wanted_hex_len + wanted_numeric))

length_features = df[selected_cols]
out_csv = os.path.join(base_dir, f"{os.path.splitext(file_name)[0]}_length_features.csv")
length_features.to_csv(out_csv, index=False, encoding="utf-8-sig")

print("✅ Length Features CSV:", out_csv)
