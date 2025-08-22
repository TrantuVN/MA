import pandas as pd
import json
import ast
import os

# ====================== INPUT/OUTPUT ======================
base_dir = r"C:\Users\Multiplexon\Desktop\data\2"
input_files = [

    os.path.join(base_dir, "total 2025.csv"),
]

# Cột cần drop
DROP_COLS = ["From_Nametag", "To_Nametag", "Amount", "Value (USD)", "Events"]

# -------- Helper: parse logs an toàn --------
def parse_logs_cell(cell):
    if isinstance(cell, (list, dict)):
        return cell
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    # Thử parse JSON (đổi ' -> " nếu cần)
    try:
        return json.loads(s.replace("'", '"'))
    except Exception:
        pass
    # Fallback literal_eval
    try:
        return ast.literal_eval(s)
    except Exception:
        return []

def extract_userop_event(logs_cell):
    """
    Từ logs, tìm event == 'UserOperationEvent' và lấy:
    sender, paymaster, logIndex, actualGasCost, actualGasUsed, nonce, success
    (Ưu tiên trong args, fallback ngoài log)
    """
    logs = parse_logs_cell(logs_cell)
    if isinstance(logs, dict):
        logs = [logs]
    if not isinstance(logs, list):
        return {}

    for log in logs:
        if isinstance(log, dict) and str(log.get("event", "")).strip() == "UserOperationEvent":
            args = log.get("args", {}) or {}
            return {
                "sender": args.get("sender") or log.get("sender"),
                "paymaster": args.get("paymaster") or log.get("paymaster"),
                "logIndex": log.get("logIndex"),
                "actualGasCost": args.get("actualGasCost") or log.get("actualGasCost"),
                "actualGasUsed": args.get("actualGasUsed") or log.get("actualGasUsed"),
                "nonce": args.get("nonce") or log.get("nonce"),
                "success": args.get("success") or log.get("success"),
            }
    return {}

# ---------------------- Xử lý từng file ----------------------
processed_paths = []
for file in input_files:
    df = pd.read_csv(file)

    # 1) Drop các cột không cần
    df2 = df.drop(columns=DROP_COLS, errors="ignore").copy()

    # 2) Tách thông tin từ logs (event: UserOperationEvent)
    if "logs" in df2.columns:
        extracted = df2["logs"].apply(extract_userop_event)
        extracted_df = pd.json_normalize(extracted)
        final_df = pd.concat([df2, extracted_df], axis=1)
    else:
        # Nếu không có cột logs thì vẫn lưu ra phần đã drop cột
        final_df = df2

    # 3) Lưu file đã xử lý (không lọc theo Status/Method, giữ nguyên cột logs)
    base, ext = os.path.splitext(file)
    out_path = f"{base}_processed_no_filter{ext}"
    final_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    processed_paths.append(out_path)
    print(f"✅ Đã xử lý và lưu: {out_path}")
    