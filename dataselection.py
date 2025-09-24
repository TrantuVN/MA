#!/usr/bin/env python3
# dataselection.py

import pandas as pd
import json, ast, os
from typing import Any, Dict, List, Union

# ====== INPUT ======
BASE_DIR = r"C:\Users\Multiplexon\Desktop\data\d6"
INPUT_FILES = [
    os.path.join(BASE_DIR, "combine 2025.csv"),
    # os.path.join(BASE_DIR, "combine 2025.csv"),
]

# ====== SETTINGS ======
DROP_COLS = ["From_Nametag", "To_Nametag", "Amount", "Value (USD)", "Events"]
TARGET_STATUS = "Success"
TARGET_METHOD = "handleOps(tuple[] ops,address beneficiary)"


# ---------- helpers: topics/data decode ----------
def _topic_to_address(topic_hex: str) -> str:
    if not isinstance(topic_hex, str):
        return ""
    h = topic_hex.lower()
    if h.startswith("0x"):
        h = h[2:]
    return "0x" + h[-40:] if len(h) >= 40 else ""


def _word_at(data_hex: str, i: int) -> str:
    if not isinstance(data_hex, str):
        return ""
    h = data_hex.lower()
    if h.startswith("0x"):
        h = h[2:]
    return h[64 * i : 64 * (i + 1)]


def _ihex(w: str) -> int:
    try:
        return int(w or "0", 16)
    except Exception:
        return 0


# ---------- robust parse logs ----------
def _parse_logs(cell: Any) -> Union[List, Dict]:
    """Trả list/dict từ cell 'logs' (JSON/escaped/literal)."""
    if isinstance(cell, (list, dict)):
        return cell
    if cell is None:
        return []
    s = str(cell)
    if not s or s.strip().lower() in ("nan", "none"):
        return []

    try:
        return json.loads(s)
    except Exception:
        pass

    try:
        lit = ast.literal_eval(s)
        if isinstance(lit, (list, dict)):
            return lit
    except Exception:
        pass

    if s.startswith('"') and s.endswith('"'):
        inner = s[1:-1]
        try:
            inner = inner.encode("utf-8").decode("unicode_escape")
            return json.loads(inner)
        except Exception:
            pass
    return []


def _extract_uoe_from_log_item(item: Dict[str, Any]) -> Dict[str, str]:
    """Trích dữ liệu từ 1 object UserOperationEvent"""
    args = item.get("args", {}) or {}
    out = {
        "sender": args.get("sender") or item.get("sender", ""),
        "paymaster": args.get("paymaster") or item.get("paymaster", ""),
        "actualGasCost": args.get("actualGasCost") or item.get("actualGasCost", ""),
        "actualGasUsed": args.get("actualGasUsed") or item.get("actualGasUsed", ""),
        "nonce": args.get("nonce") or item.get("nonce", ""),
        "success": args.get("success") or item.get("success", ""),
        "logIndex": item.get("logIndex", ""),
    }

    topics = item.get("topics", []) or []
    data_hex = item.get("data", "") or ""

    if not out["sender"] and len(topics) >= 3:
        out["sender"] = _topic_to_address(topics[2])
    if not out["paymaster"] and len(topics) >= 4:
        out["paymaster"] = _topic_to_address(topics[3])

    if data_hex:
        w0, w1, w2, w3 = _word_at(data_hex, 0), _word_at(data_hex, 1), _word_at(data_hex, 2), _word_at(data_hex, 3)
        if not out["nonce"]:
            out["nonce"] = str(_ihex(w0))
        if not out["success"]:
            out["success"] = "1" if _ihex(w1) != 0 else "0"
        if not out["actualGasCost"]:
            out["actualGasCost"] = str(_ihex(w2))
        if not out["actualGasUsed"]:
            out["actualGasUsed"] = str(_ihex(w3))

    for k in ("sender", "paymaster", "actualGasCost", "actualGasUsed", "nonce", "success", "logIndex"):
        v = out.get(k)
        out[k] = "" if v is None else str(v)
    return out


def _extract_uoe_fields_from_logs(logs_cell: Any) -> Dict[str, str]:
    logs = _parse_logs(logs_cell)
    if isinstance(logs, dict):
        logs = [logs]
    best, best_score = {}, -1
    if isinstance(logs, list):
        for it in logs:
            if not isinstance(it, dict):
                continue
            if str(it.get("event", "")) != "UserOperationEvent":
                continue
            cand = _extract_uoe_from_log_item(it)
            score = sum(1 for v in cand.values() if v not in ("", None))
            if score > best_score:
                best, best_score = cand, score
            if score >= 7:
                break
    for c in ("sender", "paymaster", "actualGasCost", "actualGasUsed", "nonce", "success", "logIndex"):
        if c not in best:
            best[c] = ""
    return best


def process_one_file(path: str) -> str:
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
        on_bad_lines="warn",
    )
    total = len(df)

    if "Status" in df.columns and "Method" in df.columns:
        mask = (df["Status"] == TARGET_STATUS) & (df["Method"] == TARGET_METHOD)
        df = df.loc[mask].copy()
        print(f"🧹 {os.path.basename(path)}: kept {len(df)}/{total} rows")
    else:
        print("⚠️ Missing 'Status' or 'Method' — cannot filter.")

    df = df.drop(columns=DROP_COLS, errors="ignore")

    if "logs" in df.columns:
        extracted = df["logs"].apply(_extract_uoe_fields_from_logs)
        exdf = extracted.apply(pd.Series)
        needed = ["sender", "paymaster", "actualGasCost", "actualGasUsed", "nonce", "success", "logIndex"]
        for c in needed:
            if c not in exdf.columns:
                exdf[c] = ""
        out_df = pd.concat([df.reset_index(drop=True), exdf[needed].reset_index(drop=True)], axis=1)
    else:
        out_df = df.copy()
        for c in ["sender", "paymaster", "actualGasCost", "actualGasUsed", "nonce", "success", "logIndex"]:
            out_df[c] = ""

    base, ext = os.path.splitext(path)
    out_path = f"{base}_processed_filtered.csv"
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out_path} (rows={len(out_df)}, cols={len(out_df.columns)})")
    return out_path


def main():
    for fp in INPUT_FILES:
        if not os.path.isfile(fp):
            print(f"⚠️ Not found: {fp}")
            continue
        process_one_file(fp)


if __name__ == "__main__":
    main()
