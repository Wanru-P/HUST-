"""补采 lastModified：列表接口 /api/models 不返回该字段，只有详情接口
/api/models/{id} 才有。本脚本读取 Bronze，对缺失 lastModified 的模型逐条
调详情接口补齐，再写回 Bronze（覆盖）。

代理：走 Windows 系统代理 127.0.0.1:7897（Clash 类），与 Chrome 同通道。
控速 + 429 退避，避免触发 HF 匿名限流。
"""
import json
import os
import sys
import time
import requests

PROXY = "http://127.0.0.1:7897"
ROOT = os.path.dirname(os.path.abspath(__file__))
BRONZE = os.path.join(ROOT, "data", "bronze", "models.jsonl")
MIN_INTERVAL = 0.3
MAX_RETRIES = 6


def fetch_last_modified(mid):
    url = f"https://huggingface.co/api/models/{mid}"
    last = 0.0
    for attempt in range(MAX_RETRIES):
        elapsed = time.time() - last
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        try:
            r = requests.get(url, headers={"User-Agent": "hf_eco_analysis/1.0"},
                             proxies={"http": PROXY, "https": PROXY}, timeout=25)
        except requests.RequestException as e:
            wait = (2 ** attempt) + 1
            print(f"[warn] {mid} 网络错误 {e}；{wait:.1f}s 后重试", file=sys.stderr)
            time.sleep(wait); last = time.time(); continue
        if r.status_code == 429:
            wait = (2 ** attempt) + 1
            print(f"[429] {mid} 被限流；休眠 {wait:.1f}s", file=sys.stderr)
            time.sleep(wait); last = time.time(); continue
        if r.status_code != 200:
            print(f"[err] {mid} HTTP {r.status_code}: {r.text[:120]}", file=sys.stderr)
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) + 1
                time.sleep(wait); last = time.time(); continue
            return None
        try:
            d = r.json()
            last = time.time()
            return d.get("lastModified")
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def main():
    rows = []
    with open(BRONZE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    print(f"[enrich] 载入 {len(rows)} 条 Bronze")
    updated = 0
    for i, m in enumerate(rows, 1):
        if m.get("lastModified"):
            continue
        lm = fetch_last_modified(m["id"])
        if lm:
            m["lastModified"] = lm
            updated += 1
        if i % 50 == 0:
            print(f"[enrich] 进度 {i}/{len(rows)} 已补 {updated}")
    with open(BRONZE, "w", encoding="utf-8") as f:
        for m in rows:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    print(f"[enrich] 完成：为 {updated} 个模型补齐 lastModified，已写回 Bronze")


if __name__ == "__main__":
    main()
