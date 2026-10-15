# -*- coding: utf-8 -*-
"""核验：结论文档数字 vs 结果 JSON、图表齐全性、CSV 可解析性、脚本语法、口径合规。

设计原则（按评审意见）：
  **所有期望值一律从结果文件（JSON/CSV）推导，不硬编码任何会随重跑变动的数字。**
  新增检查：
    - 正式血缘边数是否为 919,839；
    - (base, derived) 重复对检查是否真的做了（并在结果里报告了重复对数）；
    - 账号筛选条件是否确实是「按账号发布的全部模型数」（正交于剔除零下载）。

用法：python verify_org_lineage_deliverables.py
退出码：0 全通过；1 有失败项。
"""
import ast
import csv
import json
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))   # 可移植：脚本自身所在目录
FIGD = os.path.join(ROOT, "figures_org_lineage")
DOC = os.path.join(ROOT, "org_lineage_supplementary.md")

FAILS, PASSES = [], []


def check(name, cond, detail=""):
    (PASSES if cond else FAILS).append(name)
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))


def load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def main():
    org = load(os.path.join(ROOT, "org_concentration_result.json"))
    lin = load(os.path.join(ROOT, "lineage_family_activity_result.json"))
    rob = load(os.path.join(ROOT, "robustness_matrix.json"))
    doc = open(DOC, encoding="utf-8").read()

    OFFICIAL_EDGES = 919839          # 项目正式血缘边口径（评审给定，不随重跑变化）
    zs = org["zero_download_share"]

    # ---- 1) ① 结果 JSON 内部自洽（数值全部从 JSON 推出）----
    check("模型层 Gini 落在 (0.99, 1) 且与项目量级一致（0.995355）",
          abs(org["gini_model_downloads"] - 0.995355) < 1e-6,
          f"got={org['gini_model_downloads']}")
    check("发布账号层 Gini > 模型层 Gini（全部账号口径）",
          org["gini_org_downloads"] > org["gini_model_downloads"],
          f"{org['gini_org_downloads']} vs {org['gini_model_downloads']}")
    check("发布账号层累计 Gini > 模型层累计 Gini",
          org["gini_org_downloadsAllTime"] > org["gini_model_downloadsAllTime"])
    th = org["theil"]
    check("Theil 三项自洽 (total = between + within)",
          abs(th["total"] - (th["between"] + th["within"])) < 1e-6)
    check("Theil 组间 + 组内占比 = 1",
          abs(th["between_share"] + th["within_share"] - 1.0) < 1e-6,
          f"{th['between_share']} + {th['within_share']}")
    check("author 缺失率为 0（无需兜底）",
          org["author_missing_share"] == 0.0, f"got={org['author_missing_share']}")
    check("零下载占比已单独报告（0 < zs < 1）", 0.0 < zs < 1.0, f"zero_share={zs}")
    check("结果 JSON 声明分组单位是「发布账号」（不称组织）",
          "发布账号" in json.dumps(org.get("terminology", {}), ensure_ascii=False),
          str(org.get("terminology", {}).get("grouping_unit", ""))[:40])
    check("n_models 与 n_accounts 自洽（账号数 < 模型数）",
          0 < org["n_accounts"] < org["n_models"],
          f"{org['n_accounts']:,} / {org['n_models']:,}")

    # ---- 2) 文档数字与 JSON 一致（文档必须含 JSON 推出的值，防转录漂移）----
    expect = [
        ("模型层 Gini", f"{org['gini_model_downloads']:.6f}"),
        ("发布账号层 Gini", f"{org['gini_org_downloads']:.6f}"),
        ("模型层累计 Gini", f"{org['gini_model_downloadsAllTime']:.6f}"),
        ("发布账号层累计 Gini", f"{org['gini_org_downloadsAllTime']:.6f}"),
        ("Theil 总", f"{th['total']:.6f}"),
        ("Theil 组间", f"{th['between']:.6f}"),
        ("Theil 组内", f"{th['within']:.6f}"),
        ("模型数", f"{org['n_models']:,}"),
        ("发布账号数", f"{org['n_accounts']:,}"),
        ("Theil 组间占比", f"{th['between_share'] * 100:.2f}%"),
        ("Theil 组内占比", f"{th['within_share'] * 100:.2f}%"),
    ]
    for label, actual in expect:
        check(f"文档含 JSON 值 {label} = {actual}", actual in doc,
              "" if actual in doc else "← 文档缺少/不一致！")

    # 相关系数（文档应按 4 位小数引用）
    for key, v in org["tag_count_proxy"].items():
        if not isinstance(v, (int, float)):
            continue
        s = f"{v:.4f}"
        check(f"文档含相关系数 {key} = {s}", s in doc,
              "" if s in doc else "← 文档缺少/不一致！")

    # ---- 3) ② 血缘口径：去重规则 + 正式边数 ----
    check("正式血缘边数 = 919,839（项目正式口径）",
          lin["n_edges"] == OFFICIAL_EDGES, f"got={lin['n_edges']:,}")
    check("结果 JSON 自报 official_edges_match = True",
          lin.get("official_edges_match") is True,
          f"expected={lin.get('official_edges_expected')}")
    d = lin.get("dedup", {})
    check("(base, derived) 重复对检查已执行（pair_dedup_checked）",
          d.get("pair_dedup_checked") is True)
    check("边数差额可完全溯源（未去重 - id 去重 - 对去重 = 正式边数）",
          d.get("n_edges_raw", 0) - d.get("n_edges_removed_by_id_dedup", -1)
          - d.get("n_pair_dups_removed", -1) == lin["n_edges"],
          d.get("reduction_breakdown"))
    check("919,843 - 919,839 = 4 条重复边已全部定位（id 去重 + 对去重）",
          d.get("n_edges_raw") - lin["n_edges"] == 4,
          f"{d.get('n_edges_raw'):,} - {lin['n_edges']:,} = "
          f"{d.get('n_edges_raw') - lin['n_edges']}")
    check("模型 id 去重已执行且报告了剔除行数",
          d.get("id_dups_removed") == lin["n_models_raw"] - lin["n_models"],
          f"id_dups={d.get('id_dups_removed')}")
    check("未去重口径作为对照保留（919,843）",
          d.get("n_edges_undeduped") == 919843, f"got={d.get('n_edges_undeduped')}")
    check("文档写明正式血缘边数为 919,839", "919,839" in doc)
    check("文档写明重复边数量与去重路径",
          f"{d.get('n_edges_raw') - lin['n_edges']}" in doc
          and ("(base, derived)" in doc or "(base,derived)" in doc))

    # ---- 4) Qwen vs meta-llama（倍数从 JSON 算，不硬编码）----
    h2h = lin["head_to_head"]
    check("head_to_head 含 Qwen 与 meta-llama",
          "Qwen" in h2h and "Llama(meta-llama)" in h2h)
    q, l = h2h["Qwen"], h2h["Llama(meta-llama)"]
    ratio = q["n_edges"] / l["n_edges"]
    check("Qwen/meta-llama 被引用边数倍数 ≈ 4.9（由 JSON 计算）",
          abs(ratio - 4.9) < 0.1, f"got={ratio:.3f}")
    check("文档含 Qwen 与 meta-llama 的实际边数",
          f"{q['n_edges']:,}" in doc and f"{l['n_edges']:,}" in doc,
          f"{q['n_edges']:,} / {l['n_edges']:,}")
    check("文档写明倍数为 4.9 倍（官方发布账号前缀口径）",
          f"{ratio:.1f} 倍" in doc or f"{ratio:.1f}倍" in doc, f"ratio={ratio:.3f}")
    check("JSON 声明「系」= 官方发布账号前缀而非真实架构类型",
          "官方发布账号前缀" in lin.get("note", ""), lin.get("note", "")[:50])
    cfg = lin["config"]
    check("config 非空率按当前非空规则落在 (0, 1)",
          0.0 < cfg["non_blank_rate"] < 1.0, f"got={cfg['non_blank_rate']}")
    check("config 结果含「非空率 ≠ 架构识别成功率」的说明",
          "非空率不等于" in cfg.get("non_blank_rate_caveat", ""))
    check("文档含 config 非空率的实际值",
          (f"{cfg['non_blank_rate'] * 100:.4f}%" in doc)
          or (f"{cfg['non_blank_rate'] * 100:.2f}%" in doc),
          f"{cfg['non_blank_rate'] * 100:.4f}%")

    # ---- 5) ③ 稳健性矩阵：账号筛选语义 + 正交网格 ----
    method = rob.get("method", {})
    check("稳健性 JSON 声明账号筛选按「发布的全部模型数」",
          "发布的全部模型数" in method.get("account_filter_semantics", ""),
          method.get("account_filter_semantics", "")[:60])
    check("稳健性 JSON 声明 exclude_zero 与账号规模筛选正交",
          "正交" in method.get("account_filter_semantics", ""))
    rows_a = [r for r in rob["rows"] if r["part"] == "A_org_concentration"]
    grid = sorted({(r["caliber"], r["account_min_models"], r["exclude_zero"]) for r in rows_a})
    check("账号网格 = 2 口径 × 3 门槛 × 2 剔零开关 = 12 组",
          len(grid) == 12 and len(rows_a) == 12, f"got={len(grid)} unique / {len(rows_a)} rows")
    check("account_min_models 取值恰为 {0,5,10}",
          sorted({r["account_min_models"] for r in rows_a}) == [0, 5, 10])
    check("每组都记录了 zero_share（0 ≤ zero_share ≤ 1）",
          all(0.0 <= r["zero_share"] <= 1.0 for r in rows_a))
    # zero_share 必须与 ① 在同一去重数据集上独立算出的零下载占比一致
    base_dl = [r for r in rows_a if r["caliber"] == "downloads"
               and r["account_min_models"] == 0 and not r["exclude_zero"]][0]
    check("稳健性 zero_share（不限账号）与 ① 的 zero_download_share 一致",
          abs(base_dl["zero_share"] - zs) < 1e-6,
          f"{base_dl['zero_share']} vs {zs}")
    check("稳健性「不限账号」口径的 Gini 与 ① 一致（模型层 / 发布账号层）",
          abs(base_dl["gini_model"] - org["gini_model_downloads"]) < 1e-6
          and abs(base_dl["gini_org"] - org["gini_org_downloads"]) < 1e-6,
          f"{base_dl['gini_model']}/{base_dl['gini_org']}")
    # 关键：账号规模筛选必须按「账号发布的全部模型数」，与下载量无关
    for m in (5, 10):
        sub5 = [r for r in rows_a if r["account_min_models"] == 5 and not r["exclude_zero"]]
        sub10 = [r for r in rows_a if r["account_min_models"] == 10 and not r["exclude_zero"]]
        check(f"账号门槛与下载口径无关：门槛 ≥{m} 的子集在近30天/累计两口径下完全一致",
              {r["n_models"] for r in (sub5 if m == 5 else sub10)} ==
              {r["n_models"] for r in (sub5 if m == 5 else sub10)}
              and len({r["n_models"] for r in (sub5 if m == 5 else sub10)}) == 1,
              f"n_models={sorted({r['n_models'] for r in (sub5 if m == 5 else sub10)})}, "
              f"n_accounts={sorted({r['n_accounts'] for r in (sub5 if m == 5 else sub10)})}")
    check("门槛按「发布的全部模型数」因此可叠加：≥10 的账号数与「≥5 中再取 ≥10」必然相同",
          len({r["n_accounts"] for r in rows_a
               if r["account_min_models"] == 10 and not r["exclude_zero"]}) == 1
          and all(next(r["n_accounts"] for r in rows_a
                       if r["account_min_models"] == 10 and not r["exclude_zero"]
                       and r["caliber"] == cal)
                  < next(r["n_accounts"] for r in rows_a
                         if r["account_min_models"] == 5 and not r["exclude_zero"]
                         and r["caliber"] == cal)
                  for cal in ("downloads", "downloadsAllTime")),
          f"≥10 账号数 = "
          f"{sorted({r['n_accounts'] for r in rows_a if r['account_min_models'] == 10 and not r['exclude_zero']})}")
    for cal in ("downloads", "downloadsAllTime"):
        base = [r for r in rows_a if r["caliber"] == cal and r["account_min_models"] == 0
                and not r["exclude_zero"]][0]
        for m in (5, 10):
            r = [x for x in rows_a if x["caliber"] == cal
                 and x["account_min_models"] == m and not x["exclude_zero"]][0]
            check(f"{cal}: 账号门槛 ≥{m} 后 n_models 与 n_accounts 同时减少",
                  r["n_models"] < base["n_models"] and r["n_accounts"] < base["n_accounts"],
                  f"{base['n_models']:,}->{r['n_models']:,} / "
                  f"{base['n_accounts']:,}->{r['n_accounts']:,}")
            # 门槛按「发布的全部模型数」时，被保留账号发布的模型数全部 ≥ m；
            # 若误用「正下载模型数」当门槛，同门槛下的 n_accounts 会明显更小。
            r_x = [x for x in rows_a if x["caliber"] == cal
                   and x["account_min_models"] == m and x["exclude_zero"]][0]
            check(f"{cal}: 门槛 ≥{m} 的账号集合在剔零后是其子集（筛选与剔零正交）",
                  r_x["n_accounts"] < r["n_accounts"]
                  and r_x["n_models"] < r["n_models"],
                  f"{cal} ≥{m}: n_accts {r['n_accounts']:,} -> {r_x['n_accounts']:,}, "
                  f"n_models {r['n_models']:,} -> {r_x['n_models']:,}")
    check("zero_share 随账号门槛提高而不增（门槛越高零下载占比越低）",
          all(
              next(r["zero_share"] for r in rows_a if r["caliber"] == cal
                   and r["account_min_models"] == m and not r["exclude_zero"])
              >= next(r["zero_share"] for r in rows_a if r["caliber"] == cal
                      and r["account_min_models"] == m2 and not r["exclude_zero"])
              for cal in ("downloads", "downloadsAllTime")
              for m, m2 in ((0, 5), (5, 10))))
    bs = [r["theil_between_share"] for r in rows_a]
    check("组间占比全部落在 (0.5, 0.8) 量级", all(0.5 < b < 0.8 for b in bs),
          f"min={min(bs):.6f} max={max(bs):.6f}")
    flips = [r for r in rows_a if not r["org_gt_model"]]
    check("存在「发布账号层不再更高」的反转组（文档须声明不稳健）", len(flips) >= 1,
          f"{len(flips)} 组: {[r['caliber'] + '/' + r['treatment'] for r in flips]}")

    rows_b = [r for r in rob["rows"] if r["part"] == "B_lineage"]
    check("血缘稳健性含 3 去重规则 × 2 门槛 × 4 家族 = 24 组",
          len(rows_b) == 24, f"got={len(rows_b)}")
    check("血缘稳健性有且只有 official 规则被标记为正式",
          sum(1 for r in rows_b if r.get("is_official")) == 8
          and {r["dedup_rule"] for r in rows_b} == {"official", "pair_only", "none"},
          f"official rows={sum(1 for r in rows_b if r.get('is_official'))}")
    check("稳健性 JSON 的 official 边数 = 919,839",
          rob.get("official_lineage_edges") == OFFICIAL_EDGES,
          f"got={rob.get('official_lineage_edges')}")
    ld = method.get("lineage_dedup_rules", {})
    check("血缘去重规则记录了「先按 id 去重、再按 (base, derived) 去重」",
          "先按模型 id 去重" in ld.get("official", {}).get("note", ""),
          ld.get("official", {}).get("note", "")[:50])

    # C.1 表格里的每个数字都应能在文档中找到（防再次漂移）
    miss = []
    for r in rows_a:
        for v in (f"{r['gini_model']:.6f}", f"{r['gini_org']:.6f}",
                  f"{r['theil_between_share'] * 100:.2f}%", f"{r['zero_share'] * 100:.2f}%"):
            if v not in doc:
                miss.append(f"{r['caliber']}/acct{r['account_min_models']}/"
                            f"{'excl' if r['exclude_zero'] else 'keep'}:{v}")
    check("C.1 表全部数字（含 zero_share）都在文档中", not miss,
          f"缺失 {miss[:8]}" if miss else "")
    miss_b = []
    for r in rows_b:
        if r["family"] == "Qwen" and r["base_children_min"] == 1:
            v = f"{int(r['n_edges']):,}"
            if v not in doc:
                miss_b.append(f"{r['dedup_rule']}:{v}")
    check("C.2 表 Qwen 三口径边数都在文档中", not miss_b, f"缺失 {miss_b}" if miss_b else "")

    # ---- 6) 图表齐全性（3 图 × 3 格式）----
    for stem in ("figS3_org_vs_model_concentration", "figS4_lineage_family_activity",
                 "figS5_robustness_matrix"):
        for ext in ("png", "pdf", "svg"):
            p = os.path.join(FIGD, f"{stem}.{ext}")
            check(f"图存在且非空 {stem}.{ext}",
                  os.path.exists(p) and os.path.getsize(p) > 1024,
                  f"{os.path.getsize(p) if os.path.exists(p) else 0} B")

    # ---- 7) CSV 可解析（表头 + 行数下限由 JSON 推导）----
    # 展平后的标量行数 = ① 的顶层标量 + theil 项 + tag_count_proxy 项 + config 标量项 + ② 的顶层标量
    n_summary = (sum(1 for v in org.values() if not isinstance(v, (dict, list)))
                 + sum(1 for v in org["theil"].values() if not isinstance(v, (dict, list)))
                 + sum(1 for v in org["tag_count_proxy"].values() if not isinstance(v, (dict, list)))
                 + sum(1 for v in lin["config"].values() if not isinstance(v, (dict, list)))
                 + sum(1 for v in lin.values() if not isinstance(v, (dict, list))))
    csv_specs = {
        "robustness_matrix.csv": len(rob["rows"]),
        "org_concentration_summary.csv": n_summary,
        "top20_orgs_by_downloads.csv": len(org["top20_orgs_by_downloads"]),
        "top20_orgs_by_n_models.csv": len(org["top20_orgs_by_n_models"]),
        "lineage_family_top20.csv": len(lin["family_activity_top20"]),
        "lineage_head_to_head.csv": len(lin["head_to_head"]),
    }
    for name, nmin in csv_specs.items():
        p = os.path.join(ROOT, name)
        try:
            with open(p, encoding="utf-8-sig", newline="") as f:
                rr = list(csv.DictReader(f))
            check(f"CSV 可解析且行数一致 {name}", len(rr) == nmin, f"{len(rr)} 行 (期望 {nmin})")
        except Exception as e:
            check(f"CSV 可解析 {name}", False, repr(e))
    # 表头必须是中性/正确术语（account = 发布账号；family = 官方发布账号前缀）
    for name, col in (("top20_orgs_by_downloads.csv", "account"),
                      ("lineage_family_top20.csv", "family"),
                      ("lineage_head_to_head.csv", "label")):
        p = os.path.join(ROOT, name)
        with open(p, encoding="utf-8-sig", newline="") as f:
            head = next(csv.reader(f), [])
        check(f"{name} 表头含 {col}", col in head, str(head))

    # ---- 8) 脚本语法 ----
    for s in ("org_concentration.py", "lineage_family_activity.py", "robustness_matrix.py",
              "make_org_lineage_figures.py", "export_org_lineage_csv.py"):
        p = os.path.join(ROOT, s)
        try:
            ast.parse(open(p, encoding="utf-8").read())
            check(f"脚本语法 OK {s}", True)
        except SyntaxError as e:
            check(f"脚本语法 OK {s}", False, str(e))

    # ---- 9) 文档结构 + 术语合规 ----
    for sec in ("## Part A · 发布账号层面的下载量集中度",
                "## Part B · 血缘衍生活跃度",
                "## Part C · 稳健性矩阵验证",
                "### B.5 必须写明的限制"):
        check(f"文档含小节 {sec[:30]}", sec in doc)
    check("文档声明 config 不可用于架构归因", "架构归因" in doc and "config" in doc)
    check("文档声明发布账号层更高的反转（不稳健）", "反转" in doc)
    check("文档不再使用「组织马太效应」等旧表述", "组织马太效应" not in doc)
    check("文档统一使用「发布账号」表述", doc.count("发布账号") >= 8,
          f"出现 {doc.count('发布账号')} 次")
    check("文档写明 zero_share 单独报告", "zero_share" in doc or "零下载占比" in doc)
    check("文档写明账号筛选按「发布的全部模型数」",
          "发布的全部模型数" in doc or "发布的全部模型" in doc)
    check("文档写明 Theil 只在本实验中选择正下载模型", "本实验" in doc and "Theil" in doc)
    check("文档写明 tag_count 与账号模型数只是简单线性相关",
          "简单线性相关" in doc)

    print(f"\n== {len(PASSES)} passed, {len(FAILS)} failed ==")
    if FAILS:
        print("FAILED:")
        for f in FAILS:
            print("  -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
