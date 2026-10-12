# -*- coding: utf-8 -*-
"""Part D · RQ2×RQ3 补充：底座「衍生热度」与「下载热度」是否脱钩。

问题：被大量用作底座（衍生数高）的模型，是否也是被下载最多的模型？
若二者秩相关很弱，则「被复用为底座」与「被用户下载」是两种不同的流行度，
可作为报告「下载量≠质量」「PageRank≠质量」的量化注脚。

口径：
  children_count   = 以该模型为底座（src）的衍生边数（去重 id 层面，与报告 919,839 边一致）
  downloads        = 该模型自身近 30 天下载量
  downloadsAllTime = 该模型自身历史累计下载量
相关只在「快照内底座」（children_count≥1 且有自身下载量）上计算，外部父模型无自身下载、不参与。
全量模型表另含 children_count=0（从未被用作底座）的模型，用于双向头部对照。

用法：
    HF_BRONZE_PATH=/path/to/models.parquet python supplementary/base_derivation_vs_downloads.py
    （可用 SUPP_OUT_DIR 覆盖输出目录，默认 supplementary/）
输出：<SUPP_OUT_DIR>/base_derivation_vs_downloads_result.json
依赖：pandas / numpy / pyarrow（Spearman 自算，不需 scipy）
"""
import json
import os

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.environ.get(
    "HF_BRONZE_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet"))
# 输出目录可用 SUPP_OUT_DIR 覆盖（沙箱/CI 下仓库子目录不可写时使用）
OUT_DIR = os.environ.get("SUPP_OUT_DIR", os.path.join(PROJECT_ROOT, "supplementary"))
OUT_PATH = os.path.join(OUT_DIR, "base_derivation_vs_downloads_result.json")
SNAPSHOT_DATE = pd.Timestamp("2026-10-11", tz="UTC")


def extract_base_ids(bm):
    """baseModels: {relation, models:[{_id,id},...]} -> list[str] 底座 id。
    ⚠️ pyarrow 读出的 `models` 是 numpy object array，须接受 list/tuple/ndarray。"""
    if bm is None:
        return []
    if isinstance(bm, dict):
        models = bm.get("models")
        if isinstance(models, (list, tuple, np.ndarray)):
            return [str(m["id"]) for m in models
                    if isinstance(m, dict) and m.get("id")]
    return []


def spearman_rho(x, y):
    """Spearman 秩相关 = 秩上的 Pearson 相关。

    自己算而不用 pandas 的 method="spearman"：后者内部 `from scipy.stats import spearmanr`，
    会给脚本平添 scipy 依赖（本机未装）。rank(method="average") 的并列处理与 scipy 一致。
    """
    xr = pd.Series(np.asarray(x)).rank().to_numpy(dtype=float)
    yr = pd.Series(np.asarray(y)).rank().to_numpy(dtype=float)
    return float(np.corrcoef(xr, yr)[0, 1])


def main():
    print("[1/4] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["id", "downloads", "downloadsAllTime", "baseModels"])
    raw = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    print(f"      去重后行数 = {len(raw):,}", flush=True)

    print("[2/4] 重建血缘边（src=底座, dst=衍生）...", flush=True)
    edges = [(base, mid) for mid, bm in zip(raw["id"], raw["baseModels"])
             for base in extract_base_ids(bm)]
    edf = pd.DataFrame(edges, columns=["src", "dst"])
    children = edf.groupby("src").size().rename("children_count")
    n_edges = len(edf)
    n_bases = len(children)
    print(f"      边数 = {n_edges:,}  不同底座 = {n_bases:,}", flush=True)

    print("[3/4] 全量模型 children_count × 自身下载 ...", flush=True)
    # ⚠️ 不能写成 pd.DataFrame({...Series...}, index=raw["id"])：
    #    那些 Series 带 RangeIndex，传入字符串 index 后会按 id 重新对齐 → 全 NaN。
    #    必须用 .to_numpy() 剥掉索引，再 set_index。
    dl = pd.DataFrame({
        "id": raw["id"].to_numpy(),
        "downloads": pd.to_numeric(raw["downloads"], errors="coerce").fillna(0).to_numpy(),
        "downloadsAllTime": pd.to_numeric(raw["downloadsAllTime"], errors="coerce").fillna(0).to_numpy(),
    }).set_index("id")
    allm = dl.join(children.to_frame("children_count"), how="left")
    allm["children_count"] = allm["children_count"].fillna(0).astype("int64")
    bases = allm[allm["children_count"] >= 1].copy()
    n_bases_in_snapshot = len(bases)
    print(f"      快照内底座 = {n_bases_in_snapshot:,}（外部父 = {n_bases - n_bases_in_snapshot:,}）", flush=True)

    print("[4/4] 秩相关 + 双向头部对照 ...", flush=True)
    rho_30d = spearman_rho(bases["children_count"], bases["downloads"])
    rho_all = spearman_rho(bases["children_count"], bases["downloadsAllTime"])
    lc = np.log1p(bases["children_count"].to_numpy(dtype=float))
    ld30 = np.log1p(bases["downloads"].to_numpy(dtype=float))
    ldall = np.log1p(bases["downloadsAllTime"].to_numpy(dtype=float))
    pearson_log_30d = float(np.corrcoef(lc, ld30)[0, 1])
    pearson_log_all = float(np.corrcoef(lc, ldall)[0, 1])
    print(f"      Spearman(children, downloads30d)     = {rho_30d:+.4f}  (n={len(bases):,})", flush=True)
    print(f"      Spearman(children, downloadsAllTime) = {rho_all:+.4f}", flush=True)
    print(f"      Pearson(log1p children, log1p d30d)  = {pearson_log_30d:+.4f}", flush=True)
    print(f"      Pearson(log1p children, log1p dAll)  = {pearson_log_all:+.4f}", flush=True)

    top_by_children = bases.nlargest(10, "children_count").reset_index()
    top_by_children.columns = ["base_model", "downloads", "downloadsAllTime", "children_count"]
    top_by_children = top_by_children[["base_model", "children_count", "downloads", "downloadsAllTime"]]

    top_by_dl = allm.nlargest(10, "downloads").reset_index()
    top_by_dl.columns = ["base_model", "downloads", "downloadsAllTime", "children_count"]
    top_by_dl = top_by_dl[["base_model", "children_count", "downloads", "downloadsAllTime"]]

    # 两榜交集必须由脚本「计算」，不得手写（此前曾误把
    # distilbert/distilbert-base-uncased 与 google-bert/bert-base-uncased 当成同一模型）。
    # 注意两个榜单的统计总体不同，必须在结果里标明：
    #   top10_by_children      —— 快照内底座（children_count ≥ 1）按衍生数排序
    #   top10_by_downloads30d  —— 全部模型按自身近 30 天下载量排序
    overlap_platform = sorted(set(top_by_children["base_model"]) & set(top_by_dl["base_model"]))
    # 同总体对照（都在「快照内底座」内排序），避免总体不同带来的口径歧义
    top_dl_among_bases = bases.nlargest(10, "downloads").reset_index()
    top_dl_among_bases.columns = ["base_model", "downloads", "downloadsAllTime", "children_count"]
    top_dl_among_bases = top_dl_among_bases[
        ["base_model", "children_count", "downloads", "downloadsAllTime"]]
    overlap_bases = sorted(set(top_by_children["base_model"]) & set(top_dl_among_bases["base_model"]))
    print(f"      Top10 交集（底座榜 × 全平台下载榜） = {len(overlap_platform)}  {overlap_platform}", flush=True)
    print(f"      Top10 交集（底座榜 × 底座内下载榜） = {len(overlap_bases)}  {overlap_bases}", flush=True)

    out = {
        "snapshot": str(SNAPSHOT_DATE.date()),
        "n_edges": int(n_edges),
        "n_distinct_bases": int(n_bases),
        "n_bases_in_snapshot": int(n_bases_in_snapshot),
        "n_bases_compared": int(len(bases)),
        "spearman_children_vs_downloads30d": round(float(rho_30d), 6),
        "spearman_children_vs_downloadsAllTime": round(float(rho_all), 6),
        "pearson_log1p_children_vs_log1p_downloads30d": round(pearson_log_30d, 6),
        "pearson_log1p_children_vs_log1p_downloadsAllTime": round(pearson_log_all, 6),
        "top10_by_children": top_by_children.to_dict("records"),
        "top10_by_downloads30d": top_by_dl.to_dict("records"),
        "top10_by_children_population": "快照内底座（children_count≥1，n=84,713）按衍生数排序",
        "top10_by_downloads30d_population": "全部 3,138,954 个模型按自身近 30 天下载量排序",
        "top10_overlap_with_downloads_ranking": len(overlap_platform),
        "top10_overlap_models": overlap_platform,
        "top10_by_downloads_among_bases": top_dl_among_bases.to_dict("records"),
        "top10_overlap_within_bases": len(overlap_bases),
        "top10_overlap_within_bases_models": overlap_bases,
        "note": "children_count 为以该模型为底座的衍生边数（去重 id，与报告 919,839 边一致）；downloads 为该模型自身下载量。相关只在快照内底座（children_count≥1）上计算；n 大故即使小 rho 也统计显著，应看效应量（rho 绝对值）而非 p 值。Spearman 为秩上的 Pearson（自算，无 scipy 依赖）。",
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
