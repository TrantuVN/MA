#MINMAXSCALER
cols = [
    "Transaction Hash_len","Original_len","signature_len",
    "From_len","To_len","sender_len","paymaster_len",
    "Txn Fee","Gas Used","logIndex","actualGasCost",
    "actualGasUsed","nonce","success","Blockno","DateTime_ts"
]

present = [c for c in cols if c in df.columns]
missing = [c for c in cols if c not in df.columns]
if missing:
    print("⚠️ Missing columns (không có trong df):", missing)
if not present:
    raise ValueError("Không có cột nào khớp để scale.")


X = df[present].apply(pd.to_numeric, errors="coerce")
X_imputed = SimpleImputer(strategy="median").fit_transform(X)

# 3) MinMax scale on columns
scaler = MinMaxScaler()                  # nếu dùng preprocessing: preprocessing.MinMaxScaler()
minmax_arr = scaler.fit_transform(X_imputed)

minmax = pd.DataFrame(minmax_arr, columns=present, index=df.index)
display(minmax.head())