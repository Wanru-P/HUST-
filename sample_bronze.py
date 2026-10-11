#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sample_bronze.py — 从完整 hub-stats models.parquet 中**确定性哈希抽样**约 5 万条，
作为 Phase 1A 小规模真实数据实验的 Bronze 输入。

抽样规则：crc32(model_id) % SAMPLE_MODULUS == 0
  - 确定性：同样输入永远得到同样样本（可复现）。
  - 与文件顺序无关：**不使用 head(N)**，避免落入「行组 0 = trending 块」的排序陷阱。
  - 内存友好：用 pyarrow 分批读取，不整文件载入 pandas。

用法：
    python sample_bronze.py                 # -> <root>/bronze_sample/models.parquet
    python sample_bronze.py --modulus 63
"""
import argparse
import json
import os
import sys
import time
import zlib

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from schema_adapter import SAMPLE_MODULUS, bronze_path, data_root

# 只保留 ETL 需要的列（丢弃 safetensors / siblings 等大嵌套列，省内存与磁盘）
KEEP_COLS = [
    "id", "author", "createdAt", "lastModified",
    "downloads", "downloadsAllTime", "likes",
    "pipeline_tag", "library_name", "tags", "baseModels",
]
BATCH = 200000


def select_mask(ids, modulus):
    """对一批 model_id 计算抽样掩码（确定性 crc32）。返回 list[bool]。"""
    return [(zlib.crc32(str(x).encode("utf-8")) % modulus) == 0 for x in ids]


def main():
    ap = argparse.ArgumentParser(description="确定性哈希抽样 Bronze")
    ap.add_argument("--modulus", type=int, default=SAMPLE_MODULUS)
    ap.add_argument("--src", default=None, help="源 models.parquet（默认取 bronze_path）")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    src = args.src or bronze_path()
    out = args.out or os.path.join(data_root(), "bronze_sample", "models.parquet")
    if not src.endswith(".parquet"):
        print(f"[ERROR] 抽样仅支持 parquet 源，当前为 {src}", file=sys.stderr)
        sys.exit(2)
    os.makedirs(os.path.dirname(out), exist_ok=True)

    pf = pq.ParquetFile(src)
    total = pf.metadata.num_rows
    print(f"[sample] 源: {src}  总行数: {total}  模数: {args.modulus}")

    writer = None
    kept = 0
    seen = 0
    t0 = time.time()
    for batch in pf.iter_batches(batch_size=BATCH, columns=KEEP_COLS):
        ids = batch.column("id").to_pylist()
        mask = pa.array(select_mask(ids, args.modulus), type=pa.bool_())
        seen += len(ids)
        sub = pc.filter(batch, mask)
        if sub.num_rows == 0:
            continue
        if writer is None:
            writer = pq.ParquetWriter(out, sub.schema, compression="snappy")
        writer.write_table(pa.Table.from_batches([sub]))
        kept += sub.num_rows
        print(f"\r[sample] 已扫描 {seen}/{total}  命中 {kept}", end="", flush=True)
    if writer is not None:
        writer.close()
    print()
    rate = kept / total if total else 0
    print(f"[sample] 完成：命中 {kept} 条（抽样率 {rate:.5f}），用时 {time.time()-t0:.1f}s -> {out}")

    manifest = {
        "source": src,
        "out": out,
        "method": "crc32(model_id) % modulus == 0",
        "modulus": args.modulus,
        "source_rows": total,
        "sample_rows": kept,
        "sample_rate": round(rate, 6),
        "columns": KEEP_COLS,
    }
    with open(os.path.join(os.path.dirname(out), "sample_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[sample] 抽样清单 -> {os.path.join(os.path.dirname(out), 'sample_manifest.json')}")


if __name__ == "__main__":
    main()
