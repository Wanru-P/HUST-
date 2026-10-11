#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_hubstats.py — 下载 cfahlgren1/hub-stats 的 models.parquet（经 hf-mirror.com）。

设计要点（对应 Phase 1A 要求）：
  - 固定 commit（默认 Phase 0 记录的 2c1e1950...），不用浮动的 main。
  - 支持断点续传（HTTP Range），中断后可重跑续下。
  - 下载完成后校验**完整 SHA-256**（与可信元数据一致），不是只比前缀。
  - 同时校验文件字节数。
  - 只下载 models.parquet，不碰任何模型权重。
  - 失败即退出并报错，不自动更换数据源或镜像。
  - 写出 ingest manifest（来源 / commit / 哈希 / 时间 / 行数 / 字节数）。

用法：
    python download_hubstats.py                 # 下载到 <project>/data_hubstats/bronze/models.parquet
    python download_hubstats.py --out path      # 指定输出
    python download_hubstats.py --verify-only   # 只校验已存在文件
"""
import argparse
import hashlib
import json
import os
import sys
import time

import requests

DEFAULT_COMMIT = "2c1e195090bbcbe1f940cfdcb4ac25b76e48da70"
# 可信元数据（来自 hf-mirror 的 /api/datasets/.../tree/main，与 HF LFS oid 一致）
EXPECTED_SHA256 = "d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef"
EXPECTED_SIZE = 1596738497
RESOLVE_TMPL = "https://hf-mirror.com/datasets/cfahlgren1/hub-stats/resolve/{commit}/models.parquet"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet")
CHUNK = 1024 * 1024


def sha256_of(path):
    """流式计算文件 SHA-256，避免整文件载入内存。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def download(url, out, expected_size):
    """带断点续传的下载。返回 (bytes_written, resumed_from)。"""
    os.makedirs(os.path.dirname(out), exist_ok=True)
    have = os.path.getsize(out) if os.path.exists(out) else 0
    if have > expected_size:
        print(f"[download] 现有文件 {have} 字节 > 期望 {expected_size}，删除后重下。", file=sys.stderr)
        os.remove(out)
        have = 0
    if have == expected_size:
        print(f"[download] 已存在完整大小文件（{have} 字节），跳过下载。")
        return have, have

    headers = {}
    mode = "wb"
    if have > 0:
        headers["Range"] = f"bytes={have}-"
        mode = "ab"
        print(f"[download] 断点续传：从 {have} 字节继续。")

    t0 = time.time()
    with requests.get(url, headers=headers, stream=True, timeout=120,
                      allow_redirects=True) as r:
        if have > 0 and r.status_code == 200:
            # 服务器忽略 Range -> 从头来，避免拼接损坏
            print("[download] 服务器未返回 206，改为从头下载。", file=sys.stderr)
            mode = "wb"
            have = 0
        elif have > 0 and r.status_code != 206:
            r.raise_for_status()
        elif have == 0:
            r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) + have
        done = have
        with open(out, mode) as f:
            for chunk in r.iter_content(chunk_size=CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                done += len(chunk)
                if total:
                    pct = done * 100.0 / total
                    print(f"\r[download] {done/1e6:8.1f} / {total/1e6:.1f} MB  ({pct:5.1f}%)",
                          end="", flush=True)
    print()
    print(f"[download] 完成：{done} 字节，用时 {time.time()-t0:.1f}s")
    return done, have


def main():
    ap = argparse.ArgumentParser(description="下载 hub-stats models.parquet")
    ap.add_argument("--commit", default=DEFAULT_COMMIT)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--url", default=None, help="自定义 resolve URL（默认 hf-mirror）")
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args()

    url = args.url or RESOLVE_TMPL.format(commit=args.commit)
    out = os.path.abspath(args.out)

    if not args.verify_only:
        size, _ = download(url, out, EXPECTED_SIZE)
        if size != EXPECTED_SIZE:
            print(f"[ERROR] 字节数不符：实际 {size}，期望 {EXPECTED_SIZE}。已停止，未更换数据源。",
                  file=sys.stderr)
            sys.exit(2)

    if not os.path.exists(out):
        print(f"[ERROR] 文件不存在：{out}", file=sys.stderr)
        sys.exit(2)

    actual = sha256_of(out)
    ok = (actual == EXPECTED_SHA256)
    print(f"[verify] size = {os.path.getsize(out)} (期望 {EXPECTED_SIZE})")
    print(f"[verify] sha256 = {actual}")
    print(f"[verify] 期望   = {EXPECTED_SHA256}")
    print(f"[verify] {'✅ 完整 SHA-256 校验通过' if ok else '❌ SHA-256 不匹配'}")

    manifest = {
        "dataset": "cfahlgren1/hub-stats",
        "file": "models.parquet",
        "endpoint": "hf-mirror.com",
        "url": url,
        "commit": args.commit,
        "size_bytes": os.path.getsize(out),
        "sha256": actual,
        "sha256_expected": EXPECTED_SHA256,
        "sha256_match": ok,
        "verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "local_path": out,
    }
    mpath = os.path.join(os.path.dirname(out), "ingest_manifest.json")
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[manifest] -> {mpath}")

    if not ok:
        print("[ERROR] 哈希不匹配，已停止。请检查通道，不要自动更换数据源。", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
