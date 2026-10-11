#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_consistency.py — Phase 5：交付物与「唯一结果清单」的一致性检查。

检查三类问题：
  A. **数值一致性**：报告 / Dashboard / LaTeX 素材 / PPT 素材里必须出现清单中的
     关键数字，且格式与清单一致；同一指标在不同交付物里不能出现不同数字。
  B. **旧 Demo 污染**：正式交付物里不得出现旧 Demo 的规模、旧环境路径、
     旧 conda 环境名等（历史阶段文档 phase0/phase1a 属过程记录，不在此列）。
  C. **产物完整性**：figure_index.json 里登记的每张图，PNG/PDF/SVG 三种格式都要存在；
     Dashboard 引用的图片必须真实存在。
  D. **旧 Demo 目录未被覆盖**。

Task E 扩展（最终交付修复轮新增）：
  E1. **GraphFrames 执行状态**：从正式结果文件读取 status / steps，与结果清单对齐；
      全部完成时禁止出现「未完成」自相矛盾描述，有未完成步骤时禁止声称「全部完成」。
  E2. **图表一致性**：Figure Index 齐全、三种格式齐备、图注在 figure_index 与
      results_manifest 之间逐字一致、Figure 7 图注数字与清单一致、两个 Dashboard
      的全部图片资源都能解析、无新旧图混用。
  E3. **生成器一致性**：生成器源码与产物中都必须保留修复标记，防止重跑回退；
      并禁止无证据的根因断言。
  E4. **数据一致性**：只读已保存的正式结果，核对守恒、Gini、Top-k、边方向、
      家族规模、train/test 划分、快照版本、双口径纪律（**不重算全量**）。
  E5. **避免硬编码**：头条数字必须由 results_manifest.json 统一提供；同时生成
      `docs/manual_review_checklist.md` —— 语义问题用人工清单复核，
      **不用关键词禁令代替语义审核**。

输出：
    控制台 PASS/FAIL
    data_hubstats/gold/analysis/consistency_check_result.json
    docs/manual_review_checklist.md

用法：python build_manifest.py && python check_consistency.py
"""
import ast
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd

import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
DOCS = os.path.join(PROJECT_ROOT, "docs")
FIG_DIR = os.path.join(PROJECT_ROOT, "figures_final")
SUPP_DIR = os.path.join(PROJECT_ROOT, "figures_supplementary")
STANDALONE = os.path.join(PROJECT_ROOT, "dashboard_standalone.html")
GF_RESULT = os.path.join(ANALYSIS, "graphframes_result.json")
CHECKLIST_OUT = os.path.join(DOCS, "manual_review_checklist.md")
MANIFEST_PATH = os.path.join(ANALYSIS, "results_manifest.json")
OUT = os.path.join(ANALYSIS, "consistency_check_result.json")

RESULTS = []

# ---------------- Task E 常量（模块级，不在函数内定义函数） ----------------

# GraphFrames 六个迭代步骤：(结果清单键, 人类可读名)
GF_STEP_KEYS = [
    ("degrees", "degrees"),
    ("connected_components", "connectedComponents"),
    ("pagerank_forward", "PageRank 正向"),
    ("pagerank_reverse", "PageRank 反向"),
    ("label_propagation", "标签传播"),
    ("merge_node_metrics", "node_metrics 合并"),
]

# 只禁止「明确断言当前未完成」的写法。像「可能跑不完（初始风险）」属于**允许**的
# 过程叙述，故意不列入关键词禁令 —— 语义问题交给人工清单，不做关键词一刀切。
GF_CONTRADICTION_PATTERNS = [
    (r"status\s*[:=]\s*[\"']?running", "GraphFrames 状态写成 running"),
    (r"超出预算的算法未完成", "「超出预算的算法未完成」"),
    (r"有算法未完成", "「有算法未完成」"),
    (r"算法未完成", "「算法未完成」"),
    (r"\d+\s*个步骤未(全部)?完成", "「N 个步骤未完成」"),
    (r"确有未完成项", "「确有未完成项」"),
]

# 无证据的根因断言（Task B3 的整改对象）
FABRICATION_PATTERNS = [
    (r"属数据源日期地板效应", "把时间戳异常聚集断言为「数据源日期地板效应」"),
    (r"批量迁移写入的同一时间戳", "无证据的根因断言「批量迁移写入的同一时间戳」"),
    (r"超出预算的算法未完成", "「超出预算的算法未完成」"),
    (r"确(属|系)自然发布高峰", "把异常聚集解读为自然发布高峰"),
]

# 同一句里出现这些词，说明该句在**描述规则**或**否定**该行为，而不是在断言，
# 例如 README 里「不写死"某算法未完成"，以免……自相矛盾」。这类句子不判为矛盾。
GF_ASSERTION_SAFE_MARKERS = [
    "不写死", "以免", "禁止", "不得", "绝不", "不会", "不声称", "不把", "不将",
    "不应", "规则", "纪律", "如出现", "若将来", "如果出现", "自相矛盾",
]

# 需要检查「无未完成自相矛盾描述 / 无根因断言」的**现役交付物**
LIVE_TEXTS = [
    "report_final.md", "dashboard_final.html", "dashboard_standalone.html",
    "docs/ppt_handoff.md", "docs/graph_analysis_report.md",
    "docs/rq_analysis_report.md", "docs/latex_report_materials.md",
    "docs/results_manifest.md", "README.md",
]

# 生成器源码里必须存在的修复标记（防止重跑时被旧模板覆盖）
GEN_SOURCE_MARKERS = {
    "write_reports.py": ["def gf_steps_summary", "def gf_status_phrase",
                         "def etl_timing_note", "def caliber_ratio",
                         "def project_script_count", "初始风险", "最终状态"],
    "make_figures.py": ["def load_manifest", "def manifest_value", "def save_supp",
                        "SUPP_DIR", "def fig07_family_network",
                        "def fig07_legacy_network", "def select_family_children",
                        "def label_names", "def draw_stat_cards"],
    "analysis_evo.py": ["无法确认根因", "不作因果推断"],
    "build_manifest.py": ["gf_unfinished"],
}

# 交付物里必须出现的修复标记（证明修复确实进入了产物）
OUTPUT_FIX_MARKERS = {
    "report_final.md": ["初始风险", "最终状态"],
    "docs/ppt_handoff.md": ["初始风险"],
    "docs/graph_analysis_report.md": ["初始风险", "最终状态"],
    "docs/rq_analysis_report.md": ["无法确认根因"],
}

# 必须由 results_manifest.json 统一提供的「头条数字」：生成器源码里
# 不得再出现它们的字符串字面量（文档字符串除外，见 literal_strings）。
HARDCODE_WATCH = [
    ("data.bronze_rows", "{:,}"),
    ("data.silver_models_rows", "{:,}"),
    ("data.edge_rows", "{:,}"),
    ("data.tag_rows", "{:,}"),
    ("rq3.n_nodes_involved", "{:,}"),
    ("rq3.max_children_count", "{:,}"),
    ("rq1.peak_count", "{:,}"),
    ("rq2.downloads.gini", "{}"),
    ("rq2.downloadsAllTime.gini", "{}"),
    ("rq3_gf.viz_subgraph.component_size", "{:,}"),
]

# 人工语义复核清单：(编号, 主题, 自动可验证的部分, 必须人工判断的部分)
CHECKLIST_ITEMS = [
    ("M1", "GraphFrames 叙述的时间顺序",
     "状态字段、六步骤齐全、无「未完成」自相矛盾写法（E1 自动检查）",
     "报告中「初始风险 → 应对策略 → 最终状态」三段是否按时间顺序呈现；"
     "是否因为算法最终成功而删掉了实施过程中真实遇到的性能挑战"),
    ("M2", "ETL 两个计时口径",
     "两个数字都在报告中出现、且各自标注了口径（E2/B2 自动检查）",
     "读者能否从表题/脚注看出 142.8 秒与 120.6 秒**不是同一个量**、"
     "不会误以为其中一个是笔误"),
    ("M3", "2022-03 时间戳异常聚集",
     "无「日期地板效应 / 批量迁移写入」等根因断言（E3 自动检查）",
     "叙述是否只陈述已观测现象、明确「根因未确认」、不写成自然发布高峰"),
    ("M4", "统计口径不可混用",
     "口径纪律表存在、双口径未相加（E4 自动检查）",
     "downloads / downloadsAllTime / children_count / direct_family_size / "
     "component_size / descendant_count_khop / maintenance_span_days 在正文中"
     "是否始终按各自定义使用，没有被当成同义词替换"),
    ("M5", "相关性与因果的边界",
     "RQ4 定位声明存在（E4 自动检查）",
     "是否明确写出：下载热度≠模型质量、PageRank≠模型质量、"
     "同期统计关联≠前瞻性预测、快照创建时间分布≠历史完整发布记录、"
     "任务标签缺失模型不视作已标注任务"),
    ("M6", "Figure 7 的双面板语义",
     "Panel A 数字来自清单、Panel B 只画真实边（E2/E4 自动检查）",
     "读者是否会把 Panel B 的「代表性局部子图」误读为完整弱连通分量；"
     "图注与标题是否在 PPT 缩放与 A4 单栏缩放下仍然可读"),
    ("M7", "旧 Demo 与正式结果的隔离",
     "旧 Demo 目录仍存在且未被覆盖（D 段自动检查）",
     "报告中是否任何位置引用了旧 Demo 的规模或数字"),
]


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""),
          flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": str(detail)})
    return bool(ok)


def fn(v, nd=0):
    return f"{v:,.{nd}f}" if isinstance(v, (int, float)) else str(v)


def fp(v, nd=2):
    return f"{v * 100:.{nd}f}%" if isinstance(v, (int, float)) else str(v)


def read_text(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def mv(metrics, key, default=None):
    """取清单里的值；不存在时返回默认值，绝不猜数。

    刻意放在**模块级**而不是 `main()` 内部：工程硬性约束不允许在函数内部
    再定义函数（原先这里是 `main()` 里的 `def v(...)` 闭包，已改为显式传参）。

    支持两种键形态：
      - 清单里的**扁平点号键**（如 `data.gold_rows.model_family_stats`）；
      - 嵌在某个字典值里的子字段（如 `rq3_gf.viz_subgraph.component_size`）。
    做法：先精确匹配完整键；失败则从右往左找最长的、真实存在的键前缀，
    再把它剩余的层级下钻进 `value` 字典。
    """
    if key in metrics:
        entry = metrics[key]
        return default if entry is None else entry.get("value", default)
    parts = key.split(".")
    for cut in range(len(parts) - 1, 0, -1):
        head = ".".join(parts[:cut])
        if head not in metrics:
            continue
        node = metrics[head].get("value")
        for p in parts[cut:]:
            if not isinstance(node, dict) or p not in node:
                return default
            node = node[p]
        return node
    return default


def load_json_file(path):
    """读取 JSON；不存在或损坏时返回 None（绝不静默当成空结果）。"""
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def read_live_texts():
    """读取现役交付物正文（含 README 与 standalone Dashboard）。"""
    out = {}
    for name in LIVE_TEXTS:
        out[name] = read_text(os.path.join(PROJECT_ROOT, name))
    return out


def docstring_nodes(tree):
    """收集所有**文档字符串** Constant 节点的 id。

    文档字符串里写统计数字（例如「919,839 行」）属于说明性文字，不算硬编码，
    因此 E5 的硬编码扫描必须把它们排除掉。
    """
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef,
                             ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None)
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                ids.add(id(body[0].value))
    return ids


def literal_strings(path):
    """收集源码里**非文档字符串**的字符串常量（含 f-string 的静态片段）。

    注释不在 AST 里，天然被排除。这样「数字必须来自 results_manifest.json」
    这条纪律才能被机器验证，而不是靠人工翻代码。
    """
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    skip = docstring_nodes(tree)
    out = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in skip):
            out.append(node.value)
    return out


def scan_patterns(texts, patterns):
    """在现役交付物里扫描禁用表述，返回命中列表（不做句子级过滤）。"""
    hits = []
    for name, t in texts.items():
        if not t:
            continue
        for pat, desc in patterns:
            if re.search(pat, t):
                hits.append(f"{name}: {desc}")
    return hits


def scan_assertions(texts, patterns, safe_markers):
    """按**句子**扫描禁用表述，并跳过「描述规则 / 否定该行为」的句子。

    直接对全文做关键词禁令，会把 README 里「不写死"某算法未完成"」这种
    **规则说明**误判成矛盾叙述 —— 这正是 E5 说的「不要用简单关键词禁令
    代替语义审核」。这里先切句，再看同一句里是否带否定 / 规则标记；
    语义判断的最终责任仍在人工清单（docs/manual_review_checklist.md）。
    """
    hits = []
    for name, t in texts.items():
        if not t:
            continue
        for sentence in re.split(r"[。；;!?！？\n]", t):
            if any(mk in sentence for mk in safe_markers):
                continue
            for pat, desc in patterns:
                if re.search(pat, sentence):
                    hits.append(f"{name}: {desc}")
                    break
    return hits


def probe_edge_direction(m):
    """在**真实 Silver 边表**上抽查边方向：底座必须出现在 src、不出现在 dst。

    这不是「按名字猜关系」，而是核对已落盘的边表本身：children_count 最大的
    底座若被反向绘制，它作为 src 的行数会是 0，而这里要求恰好等于其衍生数。
    只读 3 列（src/dst/relation），不做任何 Spark 计算。
    """
    parts = sorted(glob.glob(os.path.join(DIRS["silver"], "model_base_edges",
                                          "part-*.parquet")))
    top = mv(m, "rq3.top10_bases_by_children", []) or []
    if not parts or not top:
        return None
    probe = top[0]
    base, n_child = probe["base_model"], probe["children_count"]
    src_n = dst_n = 0
    rels = set()
    for p in parts:
        df = pd.read_parquet(p, columns=["src", "dst", "relation"])
        src_n += int((df["src"] == base).sum())
        dst_n += int((df["dst"] == base).sum())
        rels.update(df["relation"].dropna().unique().tolist())
    return {"base": base, "children_count": n_child, "as_src": src_n,
            "as_dst": dst_n, "relations": sorted(str(r) for r in rels)}


def write_manual_checklist(n_auto):
    """写出人工语义复核清单。

    E5 要求：无法自动验证的**语义**问题不能用关键词禁令替代，必须落到
    人工审阅清单。这里把清单固化成文档，避免它只存在于某次对话里。
    """
    lines = [
        "# 人工语义复核清单（Manual Review Checklist）",
        "",
        "> 本文件由 `check_consistency.py` 生成，请勿手工编辑。",
        f"> 生成时间：{datetime.now(timezone.utc).isoformat()}",
        "> 配套自动检查：见 `data_hubstats/gold/analysis/consistency_check_result.json`"
        f" 的 `n_checks` 字段（本清单生成前已完成 {n_auto} 项）。",
        "",
        "本清单只列**机器无法判定**的语义问题。自动检查能覆盖的部分已由 E1–E5 完成；",
        "这里刻意**不使用关键词禁令**替代语义审核，避免「写了正确关键词、但结论仍错」的情况。",
        "",
        "| 编号 | 主题 | 自动已覆盖 | 需要人工判断 | 复核结论 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for code, topic, auto, manual in CHECKLIST_ITEMS:
        lines.append(f"| {code} | {topic} | {auto} | {manual} | 待人工填写 |")
    lines += [
        "",
        "## 复核方式",
        "",
        "1. 打开 `report_final.md`、`docs/ppt_handoff.md`、`docs/graph_analysis_report.md`；",
        "2. 按上表逐条核对正文表述（不看关键词，看**语义**是否成立）；",
        "3. 在「复核结论」列填写「通过 / 需修改」，需要修改的写明具体位置；",
        "4. 任何一条不通过，都必须在 `docs/final_refinement_report.md` 中登记。",
        "",
    ]
    os.makedirs(DOCS, exist_ok=True)
    with open(CHECKLIST_OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return CHECKLIST_OUT


def main():
    if not os.path.exists(MANIFEST_PATH):
        raise SystemExit("缺少 results_manifest.json，请先运行 build_manifest.py")
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        payload = json.load(f)
    m = payload["metrics"]

    commit = mv(m, "meta.snapshot_commit", "")
    bronze = mv(m, "data.bronze_rows")
    silver = mv(m, "data.silver_models_rows")
    edges = mv(m, "data.edge_rows")
    tags = mv(m, "data.tag_rows")
    nodes = mv(m, "rq3.n_nodes_involved")
    maxchild = mv(m, "rq3.max_children_count")
    gini_dl = mv(m, "rq2.downloads.gini")
    gini_at = mv(m, "rq2.downloadsAllTime.gini")
    top1 = mv(m, "rq2.downloads.top_1pct_share")
    zero = mv(m, "rq2.downloads.zero_value_share")
    peak_month = mv(m, "rq1.peak_month")
    peak_count = mv(m, "rq1.peak_count")
    unknown_share = mv(m, "rq1.unknown_task_share")
    etl_sec = mv(m, "env.etl_elapsed_seconds")

    DELIVERABLES = {
        "report_final.md": os.path.join(PROJECT_ROOT, "report_final.md"),
        "dashboard_final.html": os.path.join(PROJECT_ROOT, "dashboard_final.html"),
        "docs/latex_report_materials.md": os.path.join(DOCS, "latex_report_materials.md"),
        "docs/ppt_handoff.md": os.path.join(DOCS, "ppt_handoff.md"),
        "docs/rq_analysis_report.md": os.path.join(DOCS, "rq_analysis_report.md"),
        "docs/graph_analysis_report.md": os.path.join(DOCS, "graph_analysis_report.md"),
        "docs/ml_experiment_report.md": os.path.join(DOCS, "ml_experiment_report.md"),
        "docs/benchmark_report.md": os.path.join(DOCS, "benchmark_report.md"),
    }

    # ---------- 0. 交付物存在性 ----------
    print("\n== 0. 交付物存在性 ==")
    texts = {}
    for name, path in DELIVERABLES.items():
        t = read_text(path)
        texts[name] = t
        check(f"交付物存在：{name}", t is not None and len(t) > 200,
              f"{len(t) if t else 0} 字符")

    # ---------- A. 数值一致性 ----------
    print("\n== A. 数值一致性（关键数字必须与结果清单一致）==")
    REQUIRED = {
        "report_final.md": [
            ("源物理行数", fn(bronze)), ("去重后主表行数", fn(silver)),
            ("血缘边数", fn(edges)), ("标签长表行数", fn(tags)),
            ("参与血缘的模型（并集）", fn(nodes)),
            ("最大直接衍生数", fn(maxchild)),
            ("Gini(downloads)", str(gini_dl)), ("Gini(downloadsAllTime)", str(gini_at)),
            ("Top-1% 份额", fp(top1)), ("零下载占比", fp(zero)),
            ("峰值月", str(peak_month)), ("峰值月新增数", fn(peak_count)),
            ("任务标签缺失占比", fp(unknown_share)),
            ("快照 commit 前缀", commit[:8]),
        ],
        "dashboard_final.html": [
            ("去重后主表行数", fn(silver)), ("血缘边数", fn(edges)),
            ("参与血缘的模型（并集）", fn(nodes)),
            ("Gini(downloads)", str(gini_dl)), ("Gini(downloadsAllTime)", str(gini_at)),
            ("Top-1% 份额", fp(top1)), ("零下载占比", fp(zero)),
            ("完整 commit", commit),
        ],
        "docs/latex_report_materials.md": [
            ("源物理行数", fn(bronze)), ("去重后主表行数", fn(silver)),
            ("血缘边数", fn(edges)), ("标签长表行数", fn(tags)),
            ("Gini(downloads)", str(gini_dl)), ("Gini(downloadsAllTime)", str(gini_at)),
            ("参与血缘的模型（并集）", fn(nodes)),
            ("最大直接衍生数", fn(maxchild)),
            ("Top-1% 份额", fp(top1)), ("零下载占比", fp(zero)),
            ("完整 commit", commit),
        ],
        "docs/ppt_handoff.md": [
            ("源物理行数", fn(bronze)), ("去重后主表行数", fn(silver)),
            ("血缘边数", fn(edges)),
            ("Gini(downloads)", str(gini_dl)), ("Top-1% 份额", fp(top1)),
            ("零下载占比", fp(zero)), ("最大直接衍生数", fn(maxchild)),
            ("峰值月", str(peak_month)),
        ],
        "docs/rq_analysis_report.md": [
            ("源物理行数", fn(bronze)), ("去重后主表行数", fn(silver)),
            ("Gini(downloads)", str(gini_dl)), ("Gini(downloadsAllTime)", str(gini_at)),
            ("Top-1% 份额", fp(top1)), ("零下载占比", fp(zero)),
            ("峰值月", str(peak_month)),
        ],
        "docs/graph_analysis_report.md": [
            ("血缘边数", fn(edges)), ("参与血缘的模型（并集）", fn(nodes)),
            ("最大直接衍生数", fn(maxchild)),
        ],
        "docs/ml_experiment_report.md": [
            ("源物理行数", fn(bronze)),
        ],
        "docs/benchmark_report.md": [
            ("源物理行数", fn(bronze)),
        ],
    }
    for name, reqs in REQUIRED.items():
        t = texts.get(name)
        if t is None:
            continue
        missing = [f"{label}={val}" for label, val in reqs if val not in t]
        check(f"{name} 关键数字齐全（{len(reqs)} 项）", not missing,
              ("缺失: " + "; ".join(missing[:6])) if missing else "")

    # 同一指标不得出现互相矛盾的写法：
    # 若把 Gini 写成 0.99 / 0.995 这类粗粒度近似（清单里是 6 位小数），视为口径漂移。
    # 正则要求「完整数字 token 且小数位 ≤3」，所以 0.995355 不会误报，0.995 会报。
    print("\n== A2. 精度口径 ==")
    for name in ("report_final.md", "dashboard_final.html",
                 "docs/latex_report_materials.md", "docs/rq_analysis_report.md"):
        t = texts.get(name) or ""
        coarse = re.findall(r"Gini[^0-9]{0,20}0\.9\d{0,3}(?![\d])", t)
        has_exact = (str(gini_dl) in t) or (str(gini_at) in t)
        check(f"{name} Gini 使用清单中的完整精度值、未写成粗粒度近似",
              has_exact and not coarse,
              f"粗粒度={coarse[:3]} 含完整精度={has_exact}")

    # ---------- B. 旧 Demo 污染 ----------
    print("\n== B. 旧 Demo / 旧环境污染检查 ==")
    FORBIDDEN = [
        (r"Demo\s*[·・]\s*1000", "旧 Demo 标题「Demo · 1000」"),
        (r"local_demo_1000", "旧 Demo 的 scale 标签"),
        (r"1,000\s*个模型|1000\s*个模型", "旧 Demo 的 1000 模型规模"),
        (r"/mnt/e/", "WSL 路径"),
        (r"data_sci", "旧 conda 环境名 data_sci"),
        (r"winutils\.exe.*(已安装|安装成功)", "不应声称已安装 winutils"),
    ]
    for name, t in texts.items():
        if t is None:
            continue
        hits = [f"{desc}: {re.findall(pat, t)[:2]}" for pat, desc in FORBIDDEN
                if re.search(pat, t)]
        check(f"{name} 无旧 Demo / 旧环境残留", not hits, "; ".join(hits))

    # ---------- C. 产物完整性 ----------
    print("\n== C. 产物完整性 ==")
    idx_path = os.path.join(ANALYSIS, "figure_index.json")
    if not os.path.exists(idx_path):
        check("figure_index.json 存在", False, "未运行 make_figures.py")
    else:
        with open(idx_path, encoding="utf-8") as f:
            idx = json.load(f)
        figs = idx.get("figures") or []
        check("图数量为 12", len(figs) == 12, f"{len(figs)} 张")
        for e in figs:
            base = e["file"].rsplit(".", 1)[0]
            for ext in ("png", "pdf", "svg"):
                p = os.path.join(FIG_DIR, f"{base}.{ext}")
                check(f"{e['figure']} 存在 {ext}", os.path.exists(p), base)
        dash = texts.get("dashboard_final.html") or ""
        missing_img = [f for f in os.listdir(FIG_DIR)
                       if f.endswith(".png") and f not in dash]
        check("Dashboard 引用了全部 PNG 图", not missing_img, str(missing_img[:4]))
        check("Dashboard 不含在线外链（离线自包含）",
              not re.search(r'src="https?://', dash), "发现外链")

    # ---------- D. 旧 Demo 目录未被改动 ----------
    print("\n== D. 旧 Demo 数据未被覆盖 ==")
    for rel in ("data", "figures", "dashboard.html", "report.md"):
        p = os.path.join(PROJECT_ROOT, rel)
        check(f"旧产物仍存在：{rel}", os.path.exists(p), "")

    live = read_live_texts()

    # ==========================================================
    # ---------- E1. GraphFrames 执行状态一致性 ----------
    # ==========================================================
    print("\n== E1. GraphFrames 执行状态一致性 ==")
    gf = load_json_file(GF_RESULT)
    if gf is None:
        check("graphframes_result.json 存在（正式图计算结果）", False, "文件缺失")
    else:
        status_file = gf.get("status")
        status_manifest = mv(m, "rq3_gf.status")
        check("GraphFrames 状态：正式结果文件 = 结果清单",
              status_file == status_manifest and status_file is not None,
              f"file={status_file} manifest={status_manifest}")

        steps_file = gf.get("steps") or {}
        miss_file = [k for k, _ in GF_STEP_KEYS if k not in steps_file]
        miss_manifest = [k for k, _ in GF_STEP_KEYS
                         if mv(m, f"rq3_gf.step.{k}", None) is None]
        check("六个迭代步骤在正式结果文件中齐全", not miss_file, str(miss_file))
        check("六个迭代步骤在结果清单中齐全", not miss_manifest, str(miss_manifest))

        unfinished = mv(m, "final.gf_unfinished", None)
        all_done = (status_file == "completed" and not miss_file and not miss_manifest)
        consistent = (isinstance(unfinished, list) and not unfinished) if all_done \
            else bool(unfinished)
        check("final.gf_unfinished 与步骤完成情况一致", consistent,
              f"gf_unfinished={unfinished} 全部完成={all_done}")

        if all_done:
            hits = scan_assertions(live, GF_CONTRADICTION_PATTERNS,
                                   GF_ASSERTION_SAFE_MARKERS)
            check("全部步骤已完成时，交付物无「未完成」自相矛盾描述",
                  not hits, "; ".join(hits[:5]))
        else:
            claims = [n for n, t in live.items()
                      if t and re.search(r"全部完成|无未完成项", t)]
            check("存在未完成步骤时，交付物不得声称「全部完成」",
                  not claims, str(claims))

    # ==========================================================
    # ---------- E2. 图表一致性 ----------
    # ==========================================================
    print("\n== E2. 图表一致性 ==")
    idx = load_json_file(os.path.join(ANALYSIS, "figure_index.json"))
    if idx is None:
        check("figure_index.json 存在", False, "未运行 make_figures.py")
    else:
        figs = idx.get("figures") or []
        check("Figure Index 登记 12 张正式图", len(figs) == 12, f"{len(figs)} 张")

        miss_fmt = []
        for e in figs:
            base = e["file"].rsplit(".", 1)[0]
            for ext in ("png", "pdf", "svg"):
                if not os.path.exists(os.path.join(FIG_DIR, f"{base}.{ext}")):
                    miss_fmt.append(f"{base}.{ext}")
        check("每张正式图都有 PNG / PDF / SVG 三种输出", not miss_fmt,
              str(miss_fmt[:5]))

        supp_base = "fig07_global_network_legacy"
        miss_supp = [f"{supp_base}.{ext}" for ext in ("png", "pdf", "svg")
                     if not os.path.exists(os.path.join(SUPP_DIR, f"{supp_base}.{ext}"))]
        check("旧版 Figure 7 作为补充材料保留（三种格式）", not miss_supp,
              str(miss_supp))

        # 图注必须在 figure_index.json 与 results_manifest.json 之间**逐字一致**：
        # 这两份是分别生成的，一旦只重跑其中一个就会漂移（实测曾发生）。
        mn_figs = mv(m, "deliverables.figures", []) or []
        drift = [a.get("figure") for a, b in zip(figs, mn_figs)
                 if a.get("note") != b.get("note")]
        check("figure_index.json 图注与 results_manifest.json 逐字一致",
              len(mn_figs) == len(figs) and not drift,
              (f"漂移: {drift}" if drift else f"manifest 收录 {len(mn_figs)} 张"))

        f7 = next((e for e in figs if e.get("figure") == "Figure 7"), None)
        if f7 is None:
            check("Figure 7 存在于 Figure Index", False, "")
        else:
            note = f7.get("note", "")
            want = {"血缘边数": fn(mv(m, "rq3.n_edges")),
                    "参与节点数": fn(mv(m, "rq3.n_nodes_involved")),
                    "最大弱连通分量": fn(mv(m, "rq3_gf.viz_subgraph.component_size")),
                    "最大直接衍生": fn(mv(m, "rq3.max_children_count"))}
            bad = [f"{k}={v}" for k, v in want.items() if v not in note]
            check("Figure 7 图注数字与结果清单一致（4 项）", not bad, "; ".join(bad))
            check("Figure 7 图注声明「全量图」与「代表性局部子图」",
                  all(s in note for s in ("全量图", "代表性局部子图", "非完整继承关系")),
                  "")

        check("Figure Index 快照版本 = 结果清单快照版本",
              idx.get("snapshot_sha") == mv(m, "meta.snapshot_commit"),
              f"index={str(idx.get('snapshot_sha'))[:8]}")

        dash = live.get("dashboard_final.html") or ""
        refs = sorted(set(re.findall(r"(figures_final/[A-Za-z0-9_]+\.png)", dash)))
        miss_ref = [r for r in refs if not os.path.exists(os.path.join(PROJECT_ROOT, r))]
        check("dashboard_final.html 引用的 12 张 PNG 全部存在",
              len(refs) == 12 and not miss_ref,
              f"{len(refs)} 个引用，缺失 {miss_ref[:3]}")
        check("dashboard_final.html 不含在线外链",
              not re.search(r'src="https?://', dash), "发现外链")

        sa = live.get("dashboard_standalone.html") or ""
        n_b64 = len(re.findall(r"data:image/png;base64,", sa))
        check("dashboard_standalone.html 内嵌 12 张 PNG（base64）",
              n_b64 == 12, f"{n_b64} 张")
        check("dashboard_standalone.html 不依赖外部相对路径",
              "figures_final/" not in sa, "仍引用相对路径")
        check("dashboard_standalone.html 不含在线外链",
              not re.search(r'src="https?://', sa), "发现外链")

        check("Dashboard 主图用新版 Figure 7、未混入旧版网络图",
              ("fig07_family_network.png" in dash)
              and ("fig07_global_network_legacy" not in dash)
              and ("fig07_global_network_legacy" not in sa), "")

    # ==========================================================
    # ---------- E3. 生成器一致性（防回退） ----------
    # ==========================================================
    print("\n== E3. 生成器一致性（重跑后修复不得回退）==")
    for src, markers in GEN_SOURCE_MARKERS.items():
        t = read_text(os.path.join(PROJECT_ROOT, src))
        missing = [mk for mk in markers if t is None or mk not in t]
        check(f"{src} 保留全部修复标记（{len(markers)} 项）", not missing,
              ("缺失: " + str(missing[:4])) if missing else "")

    for name, markers in OUTPUT_FIX_MARKERS.items():
        t = texts.get(name) or ""
        missing = [mk for mk in markers if mk not in t]
        check(f"{name} 产物中包含修复标记（{len(markers)} 项）", not missing,
              ("缺失: " + str(missing)) if missing else "")

    fab = scan_assertions(live, FABRICATION_PATTERNS, GF_ASSERTION_SAFE_MARKERS)
    check("现役交付物中无无证据的根因断言 / 未完成断言", not fab, "; ".join(fab[:5]))

    # ==========================================================
    # ---------- E4. 数据一致性（仅用已保存的正式结果） ----------
    # ==========================================================
    print("\n== E4. 数据一致性（仅用已保存结果，不重算全量）==")
    cons = mv(m, "data.conservation", {}) or {}
    flags = {k: (v or {}).get("equals_n_models") for k, v in cons.items()
             if isinstance(v, dict)}
    ok_cons = all(v is True for v in flags.values() if v is not None)
    check("Bronze→Silver→Gold 月度守恒（已保存结果）", ok_cons, str(flags))

    bronze = mv(m, "data.bronze_rows")
    silver = mv(m, "data.silver_models_rows")
    dup = mv(m, "data.source_duplicate_ids")
    check("Bronze 行数 = 去重后行数 + 源重复行数",
          bronze == silver + dup, f"{bronze} vs {silver}+{dup}")

    same = {mv(m, k) for k in ("data.silver_models_rows", "data.unique_model_ids",
                               "rq1.n_models", "rq3.n_models_total", "rq4.n_rows_full",
                               "rq2.downloads.n_models", "rq2.downloadsAllTime.n_models")}
    check("去重模型数在各处口径完全一致", len(same) == 1, str(same))

    rel = mv(m, "data.edge_relation_counts", {}) or {}
    edge_ids = {sum(rel.values()), mv(m, "data.edge_rows"), mv(m, "rq3.n_edges"),
                mv(m, "rq3_gf.n_edges"),
                (mv(m, "data.silver_rows", {}) or {}).get("model_base_edges")}
    check("血缘边数 = 四种 relation 之和 = 各处记录值",
          len(edge_ids) == 1, f"{rel} -> {edge_ids}")

    gf2 = load_json_file(GF_RESULT) or {}
    node_ids = {mv(m, "rq3.n_nodes_involved"), mv(m, "rq3_gf.n_vertices_in_graph"),
                gf2.get("n_vertices_in_graph")}
    check("参与血缘节点数 = GraphFrames 顶点数（清单与结果文件一致）",
          len(node_ids) == 1, str(node_ids))

    probe = probe_edge_direction(m)
    if probe is None:
        check("血缘边方向抽查（真实边表）", False, "缺少 Silver 边表或 top10 底座")
    else:
        check("血缘边方向正确：底座在 src、不在 dst（真实边表抽查）",
              probe["as_src"] == probe["children_count"] and probe["as_dst"] == 0,
              str(probe))
        check("relation 取值仅限 finetune / adapter / quantized / merge",
              set(probe["relations"]) <= {"finetune", "adapter", "quantized", "merge"},
              str(probe["relations"]))

    for cal in ("downloads", "downloadsAllTime"):
        g = mv(m, f"rq2.{cal}.gini")
        t1 = mv(m, f"rq2.{cal}.top_1pct_share")
        t5 = mv(m, f"rq2.{cal}.top_5pct_share")
        t10 = mv(m, f"rq2.{cal}.top_10pct_share")
        check(f"{cal}：Gini∈(0,1) 且 Top-1% ≤ Top-5% ≤ Top-10%",
              (g is not None and 0 < g < 1 and t1 <= t5 <= t10 <= 1.0),
              f"gini={g} top1={t1} top5={t5} top10={t10}")

    check("家族规模表行数与口径说明齐备",
          mv(m, "data.gold_rows.model_family_stats") == 575
          and bool(mv(m, "rq5.family_note")),
          f"rows={mv(m, 'data.gold_rows.model_family_stats')}")

    sp = mv(m, "rq4.split", {}) or {}
    sm = mv(m, "rq4.sampling", {}) or {}
    check("RQ4 train/test 行数守恒、无重叠、seed=42",
          sp.get("n_train", 0) + sp.get("n_test", 0) == sm.get("n_sample")
          and sp.get("train_test_overlap") == 0 and sp.get("seed") == 42,
          f"{sp.get('n_train')}+{sp.get('n_test')} vs {sm.get('n_sample')} "
          f"overlap={sp.get('train_test_overlap')} seed={sp.get('seed')}")

    check("快照版本（commit / SHA-256）齐备且与图索引、图计算结果一致",
          bool(mv(m, "meta.snapshot_commit")) and bool(mv(m, "meta.snapshot_sha256"))
          and gf2.get("snapshot_sha") == mv(m, "meta.snapshot_commit")
          and (idx or {}).get("snapshot_sha") == mv(m, "meta.snapshot_commit"), "")

    check("下载双口径各自独立、未相加（口径说明存在）",
          bool(mv(m, "rq2.meta.metric_note"))
          and mv(m, "rq2.downloads.total_downloads")
          != mv(m, "rq2.downloadsAllTime.total_downloads"), "")

    # ==========================================================
    # ---------- E5. 避免硬编码 ----------
    # ==========================================================
    print("\n== E5. 避免硬编码（关键数字统一来自 results_manifest.json）==")
    watch = []
    for key, fmt in HARDCODE_WATCH:
        val = mv(m, key)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            watch.append((key, fmt.format(val)))
    for src in ("make_figures.py", "write_reports.py"):
        lits = literal_strings(os.path.join(PROJECT_ROOT, src))
        hits = [f"{k}={s}" for k, s in watch if any(s in lit for lit in lits)]
        check(f"{src} 未硬编码应由结果清单提供的头条数字（{len(watch)} 项）",
              not hits, "; ".join(hits[:5]))

    # 人工语义复核清单（E5 明确要求：语义问题不靠关键词禁令）
    path = write_manual_checklist(len(RESULTS))
    check("人工语义复核清单已生成", os.path.exists(path),
          os.path.relpath(path, PROJECT_ROOT))

    n_fail = sum(1 for r in RESULTS if not r["ok"])
    payload_out = {"generated_at": datetime.now(timezone.utc).isoformat(),
                   "n_checks": len(RESULTS), "n_failed": n_fail,
                   "results": RESULTS}
    os.makedirs(ANALYSIS, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload_out, f, ensure_ascii=False, indent=2)
    print(f"\n共 {len(RESULTS)} 项检查，失败 {n_fail} 项 -> {OUT}")
    if n_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
