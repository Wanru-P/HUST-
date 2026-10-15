# -*- coding: utf-8 -*-
"""② 的三项稳健性矩阵 + CSV 导出（按评审意见重做）。

对 ①② 的核心结论做多口径 / 多处理下的稳定性检验。

口径（caliber）：downloads（近 30 天） / downloadsAllTime（累计）

Part A · 发布账号层面的下载量集中度（两个**正交**维度，2 × 3 × 2 = 12 组）
    - account_min_models ∈ {0, 5, 10}
        0  = 不按账号规模筛选
        5  = 仅保留**发布账号发布的全部模型数** ≥ 5 的账号
        10 = 仅保留**发布账号发布的全部模型数** ≥ 10 的账号
      ★ 注意：筛选依据是「该账号在快照中发布的模型总数」，与下载量无关；
        旧版误用「该账号的正下载模型数」当门槛，本次已修正。
    - exclude_zero ∈ {False, True}
        是否把 download == 0 的模型剔除出统计。这是一个**独立开关**，
        与账号规模筛选正交；零下载占比 zero_share 另行单独报告。
    - zero_share：该 (caliber, account_min_models) 组合下（应用 exclude_zero 之前）
      零下载模型的占比，单独报告。

Part B · 血缘谱系排序稳健性（去重规则 × 底座门槛）
    去重规则（dedup_rule）：
      - "official"     ：先按模型 id 去重，再按 (base, derived) 对有向边去重
                         → 与项目正式血缘边口径一致（919,839 条）
      - "pair_only"    ：仅按 (base, derived) 去重，不做模型 id 去重（对照）
      - "none"         ：完全不去重（对照，最宽口径）
    底座门槛：base_children_ge1 / base_children_ge5
    ★ 正式结果只使用 "official"；"pair_only" / "none" 仅作对照，已在
      JSON/CSV 的 dedup_rule 与 is_official 字段里标注清楚。

用法：python robustness_matrix.py
输出：robustness_matrix.csv, robustness_matrix.json
"""
import csv
import json
import os

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
        "    HF_BRONZE_PATH=/path/to/models.parquet python robustness_matrix.py")


PARQUET = _find_parquet()
HERE = os.path.dirname(os.path.abspath(__file__))
CSV_OUT = os.path.join(HERE, "robustness_matrix.csv")
JSON_OUT = os.path.join(HERE, "robustness_matrix.json")

# 正式血缘边口径（项目 results_manifest.json benchmark 去重后口径）
OFFICIAL_EDGES = 919839
ACCOUNT_MIN_MODELS = (0, 5, 10)
EXCLUDE_ZERO = (False, True)
FAMILIES = ("Qwen", "meta-llama", "google", "black-forest-labs")


def gini(x):
    x = np.sort(np.asarray(x, dtype=float))
    n = x.size
    s = x.sum()
    if n == 0 or s <= 0:
        return float("nan")
    i = np.arange(1, n + 1, dtype=float)
    return float((2.0 * (i * x).sum()) / (n * s) - (n + 1.0) / n)


def theil_split(x, grp):
    """按 grp 分组做 Theil T 分解（仅正值）。返回 (total, between, within, between_share)。"""
    m = x > 0
    x = x[m]
    if x.size == 0:
        return (np.nan,) * 4
    g = pd.Series(grp[m])
    mu = x.mean()
    T = float(np.mean((x / mu) * np.log(x / mu)))
    df = pd.DataFrame({"g": g.to_numpy(), "x": x})
    agg = df.groupby("g")["x"].agg(["size", "sum", "mean"])
    s_g = agg["sum"] / agg["sum"].sum()
    Tb = float((s_g * np.log(agg["mean"] / mu)).sum())
    return T, Tb, T - Tb, (Tb / T if T else np.nan)


def base_ids(bm):
    if isinstance(bm, dict):
        ms = bm.get("models")
        if isinstance(ms, (list, tuple, np.ndarray)):
            return [str(m["id"]) for m in ms if isinstance(m, dict) and m.get("id")]
    return []


def build_edges(src, pair_dedup):
    """从 src（含 id / baseModels）构建 (base, derived) 边。

    pair_dedup=True 时再按 (base, derived) 对去重，no_pair_dups 为检出并剔除的重复对数
    （与 id 去重产生的重复区分开：这里单独统计）。
    """
    e = pd.DataFrame([(b, i) for i, bm in zip(src["id"].astype(str), src["baseModels"])
                      for b in base_ids(bm)], columns=["base", "derived"])
    n_raw = len(e)
    if pair_dedup:
        e = e.drop_duplicates(subset=["base", "derived"]).reset_index(drop=True)
    return e, n_raw, n_raw - len(e)


def build_dedup_edges(raw):
    """返回 {dedup_rule: (edges, 说明)}。

    official  = 按模型 id 去重 -> 构边 -> 按 (base, derived) 去重（正式口径）
    pair_only = 不做 id 去重 -> 构边 -> 按 (base, derived) 去重（对照）
    none      = 不做任何去重（对照）
    """
    d = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    e_off, raw_off, dup_off = build_edges(d, pair_dedup=True)
    e_pair, raw_pair, dup_pair = build_edges(raw, pair_dedup=True)
    e_none, raw_none, _ = build_edges(raw, pair_dedup=False)
    # 前一层（模型 id 去重）导致的边数减少
    dup_id_off = max(0, raw_none - raw_off)
    dup_id_pair = max(0, raw_none - raw_pair)
    return {
        "official": {"edges": e_off,
                     "note": "先按模型 id 去重，再按 (base, derived) 对有向边去重（正式口径）",
                     "n_before_pair_dedup": int(raw_off),
                     "n_pair_dups_removed": int(dup_off),
                     "n_dups_removed_by_id_dedup": int(dup_id_off),
                     "dedup_source_rows": int(len(d))},
        "pair_only": {"edges": e_pair,
                      "note": "仅按 (base, derived) 去重，不做模型 id 去重（对照：剔零前属未去重口径的中间产物）",
                      "n_before_pair_dedup": int(raw_pair),
                      "n_pair_dups_removed": int(dup_pair),
                      "n_dups_removed_by_id_dedup": int(dup_id_pair),
                      "dedup_source_rows": int(len(raw))},
        "none": {"edges": e_none,
                 "note": "不做模型 id 去重、也不做 (base, derived) 去重（对照，最宽口径）",
                 "n_before_pair_dedup": int(raw_none),
                 "n_pair_dups_removed": 0,
                 "n_dups_removed_by_id_dedup": 0,
                 "dedup_source_rows": int(len(raw))},
    }


def main():
    print("[1/3] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["id", "author", "downloads", "downloadsAllTime", "baseModels"])
    n_rows_raw = len(raw)
    raw_d = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    print(f"      未去重 {n_rows_raw:,} -> 按 id 去重 {len(raw_d):,}", flush=True)

    d = pd.DataFrame({
        "account": raw_d["author"].astype(str),
        "dl": pd.to_numeric(raw_d["downloads"], errors="coerce").fillna(0).to_numpy(dtype=float),
        "dla": pd.to_numeric(raw_d["downloadsAllTime"], errors="coerce").fillna(0).to_numpy(dtype=float),
    })
    rows = []

    # ---------- ① 发布账号层面下载量集中度稳健性 ----------
    print("[2/3] ① 发布账号层面集中度稳健性矩阵（account_min_models × exclude_zero）...", flush=True)
    for cal, col in (("downloads", "dl"), ("downloadsAllTime", "dla")):
        # 账号规模只按「该账号发布的全部模型数」计算，与下载量无关
        n_pub = d.groupby("account").size()
        for nmin in ACCOUNT_MIN_MODELS:
            if nmin > 0:
                keep = set(n_pub[n_pub >= nmin].index)
                base = d[d["account"].isin(keep)]
            else:
                base = d
            # zero_share 在应用 exclude_zero 之前统计，单独报告
            zero_share = float((base[col] == 0).mean()) if len(base) else float("nan")
            for exz in EXCLUDE_ZERO:
                sub = base[base[col] > 0] if exz else base
                xx = sub[col].to_numpy()
                org_tot = sub.groupby("account")[col].sum().to_numpy()
                gm, go = gini(xx), gini(org_tot)
                T, Tb, Tw, bs = theil_split(xx, sub["account"].to_numpy())
                rows.append({
                    "part": "A_org_concentration",
                    "caliber": cal,
                    "treatment": f"acct_min{nmin}_" + ("excl_zero" if exz else "keep_zero"),
                    "account_min_models": int(nmin),
                    "exclude_zero": bool(exz),
                    "zero_share": round(zero_share, 6),
                    "n_models": int(len(sub)),
                    "n_accounts": int(sub["account"].nunique()),
                    "gini_model": round(gm, 6),
                    "gini_org": round(go, 6),
                    "org_gt_model": bool(go > gm),
                    "theil_between_share": round(bs, 6),
                })
                print(f"      {cal:<18} acct_min{nmin:<3} {'剔零' if exz else '含零'}  "
                      f"模型Gini={gm:.6f} 账号Gini={go:.6f} 组间占比={bs:.4%} "
                      f"零下载占比={zero_share:.4%} n_models={len(sub):,} n_accts={sub['account'].nunique():,}",
                      flush=True)

    # ---------- ② 血缘谱系排序稳健性（含正式去重口径） ----------
    print("[3/3] ② 血缘谱系排序稳健性矩阵（去重规则 × 底座门槛）...", flush=True)
    variants = build_dedup_edges(raw)
    for rule in ("official", "pair_only", "none"):
        v = variants[rule]
        print(f"      dedup_rule={rule:<9} 边数={len(v['edges']):,}  "
              f"(对去重前 {v['n_before_pair_dedup']:,}，剔除重复对 {v['n_pair_dups_removed']:,})", flush=True)
        e = v["edges"].copy()
        if len(e):
            e["family"] = e["base"].str.split("/").str[0]
        else:
            e["family"] = pd.Series(dtype=object)
        for thr in (1, 5):
            cnt = e.groupby("base").size() if len(e) else pd.Series(dtype=int)
            keep = set(cnt[cnt >= thr].index)
            ee = e[e["base"].isin(keep)] if len(e) else e
            n_ee = len(ee)
            for fam in FAMILIES:
                sub = ee[ee["family"] == fam] if n_ee else ee
                ne = int(len(sub))
                nb = int(sub["base"].nunique()) if ne else 0
                rows.append({
                    "part": "B_lineage",
                    "caliber": rule,
                    "dedup_rule": rule,
                    "is_official": bool(rule == "official"),
                    "treatment": f"base_children_ge{thr}",
                    "base_children_min": int(thr),
                    "family": fam,
                    "n_edges": ne,
                    "n_active_bases": nb,
                    "edges_per_active_base": round(ne / nb, 3) if nb else np.nan,
                    "share_of_all_edges": round(ne / n_ee, 6) if n_ee else np.nan,
                })
                print(f"      {rule:<9} ge{thr}  {fam:<18} 边={ne:>7,} 活跃底座={nb:>5,} "
                      f"每底座={(ne / nb if nb else 0):>7.1f}", flush=True)

    official_edges = int(len(variants["official"]["edges"]))
    print(f"      正式血缘边数（dedup_rule=official）= {official_edges:,} "
          f"（项目正式口径 {OFFICIAL_EDGES:,}）", flush=True)

    # CSV
    fields = ["part", "caliber", "treatment", "account_min_models", "exclude_zero", "zero_share",
              "n_models", "n_accounts", "gini_model", "gini_org", "org_gt_model",
              "theil_between_share", "dedup_rule", "is_official", "family", "base_children_min",
              "n_edges", "n_active_bases", "edges_per_active_base", "share_of_all_edges"]
    with open(CSV_OUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    out = {
        "method": {
            "account_filter_semantics":
                "account_min_models 按「发布账号在快照中发布的全部模型数」筛选，"
                "与下载量无关（不受零下载剔除影响）；exclude_zero 是与之正交的独立开关。",
            "account_min_models_values": list(ACCOUNT_MIN_MODELS),
            "exclude_zero_values": [bool(v) for v in EXCLUDE_ZERO],
            "account_scale_grid": len(ACCOUNT_MIN_MODELS) * len(EXCLUDE_ZERO),
            "zero_share_definition":
                "zero_share = 在 (caliber, account_min_models) 子集内、应用 exclude_zero 之前的"
                "零下载模型占比，单独报告。",
            "lineage_official_rule":
                "先按模型 id 去重 -> 构建 (base, derived) 有向边 -> 再按 (base, derived) 对去重。"
                "正式结果只使用 dedup_rule=official；pair_only / none 仅作对照。",
            "lineage_dedup_rules": {
                k: {"note": v["note"],
                    "n_edges": int(len(v["edges"])),
                    "n_before_pair_dedup": v["n_before_pair_dedup"],
                    "n_pair_dups_removed": v["n_pair_dups_removed"]}
                for k, v in variants.items()},
        },
        "official_lineage_edges": official_edges,
        "official_lineage_edges_expected": OFFICIAL_EDGES,
        "official_lineage_edges_match": bool(official_edges == OFFICIAL_EDGES),
        "n_rows_raw": int(n_rows_raw),
        "n_rows_id_dedup": int(len(raw_d)),
        "rows": rows,
    }
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {CSV_OUT}\n-> {JSON_OUT}", flush=True)


if __name__ == "__main__":
    main()
