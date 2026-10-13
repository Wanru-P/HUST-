#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_concentration_theil.py — 分组集中度（Task D）的**独立核验**。

不依赖 Spark、不重算全量：只读 `concentration_groups.json` 的**分组明细**，
用与生成脚本**不同**的路径把 Theil 分解重新算一遍，并断言三条性质：

  ① 分解恒等式：T_total = T_within + T_between（容差 1e-9）；
  ② 分组无关性：**同一口径下**，换分组维度不会改变 T_total（容差 1e-6）；
  ③ 双口径独立：downloads 与 downloadsAllTime 的 T_total 不相等。

这三条都是**数学必然**，因此一旦生成脚本写错（例如把组内/组间搞反、
把两种口径混算、或按错误的分母求均值），这里会立刻失败。

独立重算的做法：
    组均值 μ_g 用 `total_downloads / (n_models − zero_value_models)` 得到
    （即「正取值的均值」，与 Theil 只对正取值有定义的口径一致），
    s_g = 组取值和 / 总和，between = Σ s_g·ln(μ_g/μ)，
    within = Σ s_g·T_g（T_g 直接取该组已保存的 theil）。

用法：python verify_concentration_theil.py
"""
import json
import math
import os
import sys

import schema_adapter as SA

DIRS = SA.resolve_dirs()
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
PATH = os.path.join(ANALYSIS, "concentration_groups.json")

GROUPINGS = ("task", "created_year", "has_lineage")
CALIBERS = ("downloads", "downloadsAllTime")

FAILED = []


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""),
          flush=True)
    if not ok:
        FAILED.append(name)
    return ok


def recompute(per_group):
    """由分组明细独立重算 (T_total, T_within, T_between)。"""
    rows = []
    total_sum = 0.0
    n_pos = 0
    for g, st in per_group.items():
        n_positive = st["n_models"] - st["zero_value_models"]
        s = st["total_downloads"]
        if n_positive > 0 and s > 0:
            rows.append((g, s, n_positive))
            total_sum += s
            n_pos += n_positive
    if not rows or total_sum <= 0:
        return None, None, None
    grand_mean = total_sum / n_pos
    within = 0.0
    between = 0.0
    for g, s, n_positive in rows:
        s_g = s / total_sum
        between += s_g * math.log((s / n_positive) / grand_mean)
        within += s_g * per_group[g]["theil"]
    return within + between, within, between


def main():
    if not os.path.exists(PATH):
        raise SystemExit(f"缺少 {PATH}，请先运行 analysis_concentration_groups.py")
    with open(PATH, encoding="utf-8") as f:
        d = json.load(f)
    print(f"[theil] 快照 {d.get('snapshot_sha', '')[:8]} · "
          f"总模型 {d.get('n_models_total'):,}\n")

    t_by_caliber = {}
    for grouping in GROUPINGS:
        for caliber in CALIBERS:
            blk = ((d.get("groupings") or {}).get(grouping) or {}).get(caliber)
            if not blk:
                check(f"{grouping}/{caliber} 存在", False, "缺该分组结果")
                continue
            dec = blk.get("theil_decomposition") or {}
            per = blk.get("per_group") or {}
            t_total, within, between = recompute(per)
            tag = f"{grouping}/{caliber}"

            check(f"{tag}：独立重算 T_total 与保存值一致（1e-6）",
                  t_total is not None
                  and abs(t_total - float(dec["theil_total"])) < 1e-6,
                  f"indep={t_total} saved={dec.get('theil_total')}")
            check(f"{tag}：分解恒等式 T = T_within + T_between（1e-9）",
                  abs(float(dec["theil_total"])
                      - (float(dec["theil_within"]) + float(dec["theil_between"]))) < 1e-9,
                  f"{dec['theil_total']:.6f} vs "
                  f"{dec['theil_within']:.6f}+{dec['theil_between']:.6f}")
            check(f"{tag}：组内占比 + 组间占比 = 1（1e-9）",
                  abs(float(dec["between_share"]) + (1 - float(dec["between_share"])) - 1) < 1e-9,
                  f"between_share={dec['between_share']:.6f}")
            t_by_caliber.setdefault(caliber, {})[grouping] = round(float(dec["theil_total"]), 6)

    for caliber, vals in t_by_caliber.items():
        check(f"{caliber}：换分组维度不改变 T_total（分解的分组无关性）",
              len(set(vals.values())) == 1, str(vals))

    if len(t_by_caliber) == 2:
        a = list(t_by_caliber["downloads"].values())[0]
        b = list(t_by_caliber["downloadsAllTime"].values())[0]
        check("两种下载口径的 T_total 不同（口径独立、未混算）", a != b, f"{a} vs {b}")

    print()
    if FAILED:
        print(f"[theil] {len(FAILED)} 项未通过：{FAILED}")
        sys.exit(1)
    print("[theil] 全部核验通过")


if __name__ == "__main__":
    main()
