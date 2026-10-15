# -*- coding: utf-8 -*-
"""② 血缘衍生活跃度：Qwen 与 meta-llama 官方发布账号的底座复用比较 + config 字段可用率核查。

数据源：原始快照 models.parquet（baseModels 血缘 + config 元数据），不需要 Silver 层。

正式血缘边口径（与项目 results_manifest.json benchmark 的去重口径一致，共 919,839 条）：
  1) 先按模型 id 去重（源数据存在重复 id 行）；
  2) 再逐行展开 baseModels 构建 (base, derived) 有向边；
  3) 最后按 (base, derived) 对有向边去重，并报告检出/剔除的重复对数量。
  未去重（919,843）与「仅按对去重」等口径只作为**稳健性对照**，不作为正式结果。

术语：所谓「系」= **官方发布账号前缀**（模型 id 里 `org/name` 的 `org` 段，
例如 `Qwen` / `meta-llama` / `google`）。它**不是**真实架构类型归因。

用法：python lineage_family_activity.py
输出：lineage_family_activity_result.json
"""
import json
import os
from collections import Counter

import numpy as np
import pandas as pd

def _find_parquet():
    """原始快照位置：优先环境变量 HF_BRONZE_PATH，其次仓库约定位置，否则报错退出。"""
    p = os.environ.get("HF_BRONZE_PATH")
    if p and os.path.exists(p):
        return p
    here = os.path.dirname(os.path.abspath(__file__))
    for up in (here, os.path.dirname(here), os.path.dirname(os.path.dirname(here))):
        cand = os.path.join(up, "data_hubstats", "bronze", "models.parquet")
        if os.path.exists(cand):
            return cand
    raise SystemExit(
        "找不到原始快照 models.parquet。请用环境变量指定，例如：\n"
        "    HF_BRONZE_PATH=/path/to/models.parquet python lineage_family_activity.py")


PARQUET = _find_parquet()
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "lineage_family_activity_result.json")
SAMPLE_EVERY = 10          # 每 10 行取 1 行做 config 解析（约 31 万行）

# 正式血缘边口径（项目 results_manifest.json benchmark 去重后口径）
OFFICIAL_EDGES = 919839


def base_ids(bm):
    if isinstance(bm, dict):
        ms = bm.get("models")
        if isinstance(ms, (list, tuple, np.ndarray)):
            return [str(m["id"]) for m in ms if isinstance(m, dict) and m.get("id")]
    return []


def build_edges(src, pair_dedup=True):
    """构建 (base, derived) 血缘边；pair_dedup=True 时再做 (base, derived) 对去重。

    返回 (edges, n_before_pair_dedup, n_pair_dups_removed)。
    """
    e = pd.DataFrame([(b, i) for i, bm in zip(src["id"].astype(str), src["baseModels"])
                      for b in base_ids(bm)], columns=["base", "derived"])
    n_raw = len(e)
    if pair_dedup:
        e = e.drop_duplicates(subset=["base", "derived"]).reset_index(drop=True)
    return e, n_raw, n_raw - len(e)


def main():
    print("[1/4] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["id", "baseModels", "config", "downloads"])
    n_raw_rows = len(raw)
    print(f"      行数 = {n_raw_rows:,}", flush=True)

    # ---------- config 可用率 ----------
    print("[2/4] config 字段可用率（按当前非空规则）...", flush=True)
    cfg = raw["config"].astype("string")
    blank = cfg.isna() | cfg.str.strip().isin(["", "null", "None", "{}"])
    avail = 1.0 - float(blank.mean())
    print(f"      config 非空率 = {avail:.7f}（空/无效 {int(blank.sum()):,} 行）"
          f" —— 非空率不等于架构识别成功率", flush=True)
    # 抽样解析 model_type
    samp = raw.iloc[::SAMPLE_EVERY]
    mt = Counter()
    for s in samp["config"]:
        if not isinstance(s, str) or not s.strip():
            mt["<空>"] += 1
            continue
        try:
            c = json.loads(s)
        except Exception:
            mt["<解析失败>"] += 1
            continue
        m = c.get("model_type") or c.get("architectures") or "<无 model_type>"
        if isinstance(m, list):
            m = m[0] if m else "<空列表>"
        mt[str(m)] += 1
    print(f"      model_type 抽样（每 {SAMPLE_EVERY} 行取 1，n={len(samp):,}）Top10：", flush=True)
    for k, v in mt.most_common(10):
        print(f"        {k:<34} {v:>8,}", flush=True)

    # ---------- 血缘边（正式去重口径）+ 按「官方发布账号前缀」聚合 ----------
    print("[3/4] 血缘边去重 + 按官方发布账号前缀聚合 ...", flush=True)
    # 0) 对照：完全不去重（不做模型 id 去重、不做 (base, derived) 去重）
    e_raw, n_edges_raw, _ = build_edges(raw, pair_dedup=False)
    print(f"      【对照·未去重】边数 = {n_edges_raw:,}（仅作稳健性对照，不作正式结果）", flush=True)

    # 1) 按模型 id 去重
    ded = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    n_id_dups = n_raw_rows - len(ded)
    e_id, n_edges_id, _ = build_edges(ded, pair_dedup=False)
    print(f"      按 id 去重：{n_raw_rows:,} -> {len(ded):,}（剔除重复 id 行 {n_id_dups:,}）"
          f" → 构边 {n_edges_id:,} 条（较未去重减少 {n_edges_raw - n_edges_id:,} 条）", flush=True)

    # 2) 构边  3) 按 (base, derived) 对有向边去重
    e, n_edges_before, n_pair_dups = build_edges(ded, pair_dedup=True)
    n_edges = len(e)
    print(f"      按 (base, derived) 对去重：{n_edges_before:,} -> {n_edges:,}"
          f"（检出并剔除重复对 {n_pair_dups:,} 条；该步在 id 去重之后执行）", flush=True)
    print(f"      正式血缘边数 = {n_edges:,}；与项目口径 {OFFICIAL_EDGES:,} 是否一致："
          f"{'是' if n_edges == OFFICIAL_EDGES else '否'}", flush=True)
    print(f"      差额溯源：未去重 {n_edges_raw:,} - id 去重 {n_edges_raw - n_edges_id:,}"
          f" - 对去重 {n_pair_dups:,} = {n_edges:,}", flush=True)

    e["family"] = e["base"].str.split("/").str[0]
    agg = e.groupby("family").agg(n_edges=("derived", "size"),
                                  n_active_bases=("base", "nunique"))
    # 该「官方发布账号前缀」自己有多少模型、总下载
    own = ded.assign(family=ded["id"].astype(str).str.split("/").str[0]) \
             .groupby("family").agg(n_models=("id", "size"),
                                    dl=("downloads", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum())))
    agg = agg.join(own, how="outer").fillna(0)
    agg["edges_per_active_base"] = (agg["n_edges"] / agg["n_active_bases"]).replace([np.inf, -np.inf], 0)
    agg = agg.sort_values("n_edges", ascending=False)
    print("      衍生活跃度 Top12（按被引用边数）：", flush=True)
    for fam, r in agg.head(12).iterrows():
        print(f"        {fam:<18} 边={int(r.n_edges):>7,}  活跃底座={int(r.n_active_bases):>6,}  "
              f"每底座={r.edges_per_active_base:>6.1f}  该前缀模型={int(r.n_models):>7,}", flush=True)

    # ---------- Qwen vs meta-llama 正面对比 ----------
    print("[4/4] Qwen vs meta-llama 官方发布账号（显式血缘边口径）...", flush=True)
    have = set(agg.index)
    fams = {
        "Qwen": "Qwen",
        "Llama(meta-llama)": "meta-llama",
        "Gemma/BERT等(google)": "google",
        "google-bert": "google-bert",
        "Mistral": "mistralai",
        "DeepSeek": "deepseek-ai",
        "StableDiffusion(stabilityai)": "stabilityai",
        "FLUX(black-forest-labs)": "black-forest-labs",
        "GPT(openai)": "openai",
        "Microsoft(phi)": "microsoft",
    }
    head2head = {}
    for label, org in fams.items():
        if org in have:
            r = agg.loc[org]
            head2head[label] = {
                "org": org, "n_edges": int(r.n_edges),
                "n_active_bases": int(r.n_active_bases),
                "edges_per_active_base": round(float(r.edges_per_active_base), 3),
                "n_models_in_snapshot": int(r.n_models),
                "share_of_all_edges": round(float(r.n_edges) / n_edges, 6),
            }
            print(f"      {label:<30} 边={int(r.n_edges):>7,}  占比 {r.n_edges/n_edges:>7.3%}  "
                  f"活跃底座={int(r.n_active_bases):>6,}  每底座={r.edges_per_active_base:>6.1f}", flush=True)

    q, l = head2head.get("Qwen"), head2head.get("Llama(meta-llama)")
    ratio = (q["n_edges"] / l["n_edges"]) if (q and l and l["n_edges"]) else float("nan")

    out = {
        "n_models_raw": int(n_raw_rows),
        "n_models": int(len(ded)),
        "n_id_dups_removed": int(n_id_dups),
        "n_edges": int(n_edges),
        "n_edges_official": int(n_edges),
        "official_edges_expected": OFFICIAL_EDGES,
        "official_edges_match": bool(n_edges == OFFICIAL_EDGES),
        "dedup": {
            "steps": [
                "按模型 id 去重（drop_duplicates(subset=['id'])）",
                "逐行展开 baseModels 构建 (base, derived) 有向边",
                "按 (base, derived) 对去重（drop_duplicates(subset=['base','derived'])）",
            ],
            "id_dups_removed": int(n_id_dups),
            "n_edges_raw": int(n_edges_raw),
            "n_edges_after_id_dedup": int(n_edges_id),
            "n_edges_removed_by_id_dedup": int(n_edges_raw - n_edges_id),
            "n_edges_before_pair_dedup": int(n_edges_before),
            "n_pair_dups_removed": int(n_pair_dups),
            "pair_dedup_checked": True,
            "n_edges_after_dedup": int(n_edges),
            "n_edges_undeduped": int(n_edges_raw),
            "reduction_breakdown":
                f"{n_edges_raw:,}（未去重） - {n_edges_raw - n_edges_id:,}（id 去重）"
                f" - {n_pair_dups:,}（(base, derived) 对去重）= {n_edges:,}",
            "undeduped_note": "未去重口径（919,843）仅作稳健性对照，不作为正式结果。",
        },
        "n_distinct_bases": int(e["base"].nunique()),
        "qwen_over_meta_llama_edges": round(float(ratio), 3) if ratio == ratio else None,
        "config": {
            "non_blank_rate": round(avail, 7),
            "non_blank_rule": "config 为字符串且 strip 后不属于 ''/'null'/'None'/'{}' 即计为非空",
            "non_blank_rate_caveat": "非空率不等于可识别架构信息的比例；抽样中仍有大量 <无 model_type>。",
            "non_blank_rows": int((~blank).sum()),
            "blank_rows": int(blank.sum()),
            "model_type_sample_n": int(len(samp)),
            "model_type_top10": [{"model_type": k, "n": int(v)} for k, v in mt.most_common(10)],
        },
        "family_activity_top20": [
            {"family": f, "n_edges": int(r.n_edges), "n_active_bases": int(r.n_active_bases),
             "edges_per_active_base": round(float(r.edges_per_active_base), 3),
             "n_models_in_snapshot": int(r.n_models),
             "share_of_all_edges": round(float(r.n_edges) / n_edges, 6)}
            for f, r in agg.head(20).iterrows()],
        "head_to_head": head2head,
        "note": "「系」按底座 id 的官方发布账号前缀（org 段）归并，不是真实架构类型归因；"
                "衍生活跃度 = 以该前缀下模型为底座、在正式去重口径（919,839 条）下的被引用边数。"
                "config 仅做按当前非空规则的可用率与抽样 model_type 统计。",
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT}", flush=True)


if __name__ == "__main__":
    main()
