# -*- coding: utf-8 -*-
"""补充分析（Part A–E）自动校验：数字一致性 + 计数守恒 + 交付物覆盖。

用法：
    python supplementary/verify_supplementary.py
退出码：0 = 全部通过；1 = 有失败项（逐条打印 FAIL 原因）。

设计原则：
  * 只读，不写任何文件、不联网、不重算全量；
  * 每个断言都对应一处评审关注点（Top10 交集、取整口径、计数守恒、图文数字一致）。
"""
import json
import os
import re
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUPP_DIR = os.path.join(PROJECT_ROOT, "supplementary")
ANALYSIS = os.path.join(PROJECT_ROOT, "data_hubstats", "gold", "analysis")

FAILS = []
PASSES = []


def check(name, cond, detail=""):
    (PASSES if cond else FAILS).append(name)
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))


def load(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    supp = load(os.path.join(ANALYSIS, "supplementary_results.json"))
    check("supplementary_results.json 存在", supp is not None)
    if supp is None:
        return 1

    # ---- 1) Top-1% 取整口径（评审④）----
    n_models = supp["snapshot"]["n_models"]
    expect_top1 = int(n_models * 0.01)
    for metric in ("downloads", "downloadsAllTime"):
        got = supp["rq2_robustness"][metric].get("n_top1pct")
        check(f"rq2 {metric}.n_top1pct == int(N*0.01) == {expect_top1}",
              got == expect_top1, f"got={got}")

    # ---- 2) 单脚本结果 JSON 与汇总口径一致（防漂移）----
    g = load(os.path.join(SUPP_DIR, "rq2_gini_result.json"))
    if g is not None:
        for metric in ("downloads", "downloadsAllTime"):
            for k_supp, k_g in (("g_all", "g_all"), ("g_pos_excl_zero", "g_pos"),
                                ("g_top1pct_internal", "g_top")):
                a = supp["rq2_robustness"][metric].get(k_supp)
                b = (g.get(metric) or {}).get(k_g)
                check(f"rq2 {metric}.{k_supp} 汇总==脚本", a == b, f"supp={a} script={b}")

    # ---- 3) Part D：Top10 交集必须「算出来」且两榜总体写清楚（评审①）----
    d = supp["derivation_vs_downloads"]
    bd = load(os.path.join(SUPP_DIR, "base_derivation_vs_downloads_result.json"))
    if bd is not None:
        a = set(x["base_model"] for x in bd["top10_by_children"])
        b = set(x["base_model"] for x in bd["top10_by_downloads30d"])
        real = len(a & b)
        check("Part D Top10 交集由脚本计算并与汇总一致",
              real == d.get("top10_overlap_with_downloads_ranking"),
              f"real={real} supp={d.get('top10_overlap_with_downloads_ranking')}")
        check("Part D 两个榜单的统计总体已写明",
              bool(bd.get("top10_by_children_population"))
              and bool(bd.get("top10_by_downloads30d_population")))
    # 报告中不得再出现「仅重叠 1 个」这类被证伪的表述
    rep = open(os.path.join(PROJECT_ROOT, "report_final.md"), encoding="utf-8").read()
    check("report_final.md 不含被证伪的「仅重叠 1 个」", "仅重叠 1 个" not in rep)

    # ---- 4) Part E：计数守恒 + 精确 Δ==0 ≤ 整日法 0（评审④）----
    ms = supp["maintenance_span_sanity"]
    tot = ms["n_negative_span"] + ms["n_zero_span"] + ms["n_positive_span"]
    check("Part E 负/零/正 三类之和 == 时间戳齐全数", tot == ms["n_valid_timestamps"],
          f"{tot} vs {ms['n_valid_timestamps']}")
    check("Part E 精确 Δ==0 <= 整日法为 0",
          0 < ms["n_exact_zero_span"] <= ms["n_zero_span"],
          f"exact={ms['n_exact_zero_span']} intday={ms['n_zero_span']}")
    check("Part E 负跨度占比与计数自洽",
          abs(ms["negative_share"] - ms["n_negative_span"] / ms["n_valid_timestamps"]) < 1e-5)

    # ---- 5) 交付物数字一致性（图文/报告不得各说各话）----
    targets = {
        "report_final.md": os.path.join(PROJECT_ROOT, "report_final.md"),
        "dashboard_final.html": os.path.join(PROJECT_ROOT, "dashboard_final.html"),
        "dashboard_standalone.html": os.path.join(PROJECT_ROOT, "dashboard_standalone.html"),
        "docs/supplementary_analysis.md": os.path.join(PROJECT_ROOT, "docs", "supplementary_analysis.md"),
    }
    must = {
        "负跨度计数": "28,097",
        "负跨度占比": "0.895%",
        "衍生-下载秩相关": "0.37",
        "底座数 n": "84,713",
    }
    for name, path in targets.items():
        if not os.path.exists(path):
            check(f"{name} 存在", False)
            continue
        txt = open(path, encoding="utf-8").read()
        for label, pat in must.items():
            check(f"{name} 含{label}（{pat}）", pat in txt)

    # ---- 6) 生成器必须能产出补充结论（防 P0-3 回退）----
    wr = open(os.path.join(PROJECT_ROOT, "write_reports.py"), encoding="utf-8").read()
    check("write_reports.py 读取 supplementary_results.json",
          "supplementary_results.json" in wr and "def supp_blocks" in wr)
    check("write_reports.py 在报告生成中调用 supp_blocks",
          wr.count("supp_blocks(load_supplementary())") >= 5)

    print(f"\n== {len(PASSES)} passed, {len(FAILS)} failed ==")
    if FAILS:
        print("FAILED:")
        for f in FAILS:
            print("  -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
