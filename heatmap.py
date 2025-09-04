import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import os

# ====================== INPUT ======================
base_dir  = r"C:\Users\Multiplexon\Desktop\data\2"
file_name = "total_selected.csv"                 # đổi nếu cần
file_path = os.path.join(base_dir, file_name)

df = pd.read_csv(file_path)

# ====================== HELPERS ======================
def hex_to_length(x):
    """0x... -> byte length; else None"""
    try:
        s = str(x).strip()
        if s.lower().startswith("0x"):
            return len(s[2:]) // 2
    except Exception:
        pass
    return None

def hex_to_int_maybe(x):
    """'0x..' -> int; str -> int/float; misisng values -> NaN"""
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
    # seconds since epoch (UTC)
    dt = pd.to_datetime(df["DateTime (UTC)"], errors="coerce", utc=True)
    df["DateTime_ts"] = (dt.view("int64") // 10**9)

# ====================== COERCE NUMERIC ======================
numeric_targets = [
    "Txn Fee", "Gas Used", "logIndex", "actualGasCost",
    "actualGasUsed", "nonce", "success", "Blockno", "DateTime_ts"
]
for col in numeric_targets:
    if col in df.columns:
        df[col] = df[col].apply(hex_to_int_maybe)

# ====================== COLUMNS for HEATMAP ======================
len_cols = [c for c in df.columns if c.endswith("_len")]
present_numeric = [c for c in numeric_targets if c in df.columns]
all_cols = present_numeric + len_cols

num_df = df[all_cols].apply(pd.to_numeric, errors="coerce")

# remove entire NaN columns
all_nan_cols = [c for c in num_df.columns if num_df[c].isna().all()]
if all_nan_cols:
    num_df = num_df.drop(columns=all_nan_cols)

# remove constant columns
constant_cols = [c for c in num_df.columns if num_df[c].nunique(dropna=True) <= 1]
if constant_cols:
    num_df = num_df.drop(columns=constant_cols)

# checking enough columns left
if num_df.shape[1] < 2:
    raise ValueError(
        f"variance.\n"
        f"- entire NaN columns: {all_nan_cols}\n"
        f"- constant columns: {constant_cols}"
    )

# ====================== PEARSON CORRELATION ======================
corr = num_df.corr(method="pearson")

# default = 1
np.fill_diagonal(corr.values, 1)

# ====================== saving ======================
corr_csv = os.path.join(base_dir, f"{os.path.splitext(file_name)[0]}_corr_all.csv")
corr.to_csv(corr_csv, encoding="utf-8-sig")

plt.figure(figsize=(12, 9))
sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", cbar=True)
plt.title("Pearson Heatmap")
plt.tight_layout()
out_png = os.path.join(base_dir, f"{os.path.splitext(file_name)[0]}_heatmap_all.png")
plt.savefig(out_png, dpi=200)
plt.show()

print("✅ Correlation CSV:", corr_csv)
print("✅ Heatmap PNG:", out_png)

# ===========================================
# list of columns by hex
hex_cols = ["Transaction Hash", "Original", "signature", "From", "To", "sender", "paymaster"]
df.drop(columns=[c for c in hex_cols if c in df.columns], inplace=True)
#list of columns by hex_len 
wanted_hex_len = [f"{c}_len" for c in hex_cols if f"{c}_len" in df.columns]

# list of columns by numeric
extra_cols = [
    "Txn Fee", "Gas Used", "logIndex", "actualGasCost",
    "actualGasUsed", "nonce", "success", "Blockno", "DateTime_ts"
]
wanted_numeric = [c for c in extra_cols if c in df.columns]

# integrate
selected_cols = list(dict.fromkeys(wanted_hex_len + wanted_numeric))

# saving file
augmented_csv = os.path.join(base_dir, f"{os.path.splitext(file_name)[0]}_augmented.csv")
df[selected_cols].to_csv(augmented_csv, index=False, encoding="utf-8-sig")

print("✅ Augmented CSV (hex *_len + numeric):", augmented_csv)
