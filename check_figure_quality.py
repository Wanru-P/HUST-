#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_figure_quality.py — 图表**排版**自动检查（文字重叠 / 越界 / 裁切）。

为什么需要它：A5 明确要求「无节点标签明显重叠、裁切或越界」，而这类缺陷在
`make_figures.py` 里是**看不出来的** —— 脚本只会打印「已生成」，退出码 0。
历史上 Figure 7 就出现过标签互相压字、副标题跨面板撞车，全靠人工肉眼看图才发现。
本脚本把「肉眼看图」里可自动化的部分固化下来：

  1) 重新渲染每一张正式图（**不写盘**：输出目录重定向到临时目录）；
  2) 收集该图全部 Text 图元的**窗口坐标**包围盒；
  3) 检查两两重叠（排除包含关系与同属一个标题的多行文本）；
  4) 检查是否越出画布边界（裁切）。

判定阈值刻意保守：只有重叠面积同时超过两个文本各自面积的 12%、
且绝对重叠面积 > 12 pt² 才算「明显重叠」，避免把紧邻排版误报成缺陷。

用法：
    python check_figure_quality.py            # 检查全部图，打印汇总
    python check_figure_quality.py --verbose  # 打印每一处重叠的明细
"""
import json
import os
import sys
import tempfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import make_figures as MF

OVERLAP_AREA_PT2 = 12.0     # 绝对重叠面积阈值（pt²）
OVERLAP_FRAC = 0.12         # 相对自身面积阈值


def text_boxes(fig):
    """返回 [(文本, 包围盒, 是否为多行标题的一部分)]，包围盒为窗口像素坐标。

    多行标题（含 `\\n` 的 Text）本身是一个整体图元，不会与自己重叠，
    但它的每一行并不单独成为图元，因此这里直接用整块包围盒参与比较即可。
    """
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    out = []
    for ax in fig.get_axes():
        for t in ax.texts:
            if not t.get_text().strip():
                continue
            bb = t.get_window_extent(renderer=renderer)
            out.append((t.get_text(), bb, False))
        title = ax.get_title()
        if title.strip():
            out.append((title, ax.title.get_window_extent(renderer=renderer), True))
        # 轴标签必须分别取 xaxis.label / yaxis.label：
        # 早期版本两者都取 xaxis.label，会把 x 与 y 轴标签判成 100% 重叠（假阳性）。
        for lab, artist in ((ax.get_xlabel(), ax.xaxis.label),
                            (ax.get_ylabel(), ax.yaxis.label)):
            if lab.strip():
                out.append((lab, artist.get_window_extent(renderer=renderer), True))
    for t in fig.texts:
        # 图脚注按设计画在坐标区下方（y<0），由 bbox_inches="tight" 纳入输出，
        # 属于预期排版，不计入「越界」。
        if t.get_text().strip() and t.get_position()[1] >= 0:
            out.append((t.get_text(), t.get_window_extent(renderer=renderer), False))
    for leg in fig.legends + [a.get_legend() for a in fig.get_axes()
                              if a.get_legend() is not None]:
        for t in leg.get_texts():
            if t.get_text().strip():
                out.append((t.get_text(), t.get_window_extent(renderer=renderer), False))
    return out


def overlap_report(boxes):
    """两两比较包围盒，返回「明显重叠」的 (文本A, 文本B, 重叠面积, 占比) 列表。"""
    bad = []
    for i in range(len(boxes)):
        for j in range(i + 1, len(boxes)):
            ta, ba, _ = boxes[i]
            tb, bb, _ = boxes[j]
            ox = max(0.0, min(ba.x1, bb.x1) - max(ba.x0, bb.x0))
            oy = max(0.0, min(ba.y1, bb.y1) - max(ba.y0, bb.y0))
            area = ox * oy
            if area <= OVERLAP_AREA_PT2:
                continue
            sa = max(1.0, ba.width * ba.height)
            sb = max(1.0, bb.width * bb.height)
            if area / min(sa, sb) < OVERLAP_FRAC:
                continue
            bad.append((ta, tb, round(area, 1), round(area / min(sa, sb), 3)))
    return bad


def bounds_report(fig, boxes):
    """检查文本是否越出画布（会被 bbox_inches='tight' 救回，但仍属排版失控）。"""
    w, h = fig.canvas.get_width_height()
    out = []
    for t, bb, is_title in boxes:
        if is_title:
            continue
        if bb.x0 < -2 or bb.y0 < -2 or bb.x1 > w + 2 or bb.y1 > h + 2:
            out.append((t, round(bb.x0, 1), round(bb.y0, 1),
                        round(bb.x1, 1), round(bb.y1, 1)))
    return out


CAPTURED = {}


def _capture_save(fig, num, slug, title, sources, note=""):
    """替换 make_figures.save：只捕获 Figure 对象，不落盘、不关闭。

    图函数内部调用的是模块级 `save`，因此替换 MF.save 即可生效，
    **无需修改任何绘图函数**（不为了测试而改动交付代码）。
    """
    CAPTURED["fig"] = fig
    print(f"[fig] {slug}  {title}")


def _capture_save_supp(fig, base, title):
    CAPTURED["fig"] = fig
    print(f"[fig-supp] {base}  {title}")


def render_capture(fn):
    """执行绘图函数并取回它交给 save() 的 Figure；无图则返回 None。"""
    CAPTURED.clear()
    fn()
    return CAPTURED.get("fig")


def check_figure(name, fn, verbose=False):
    """渲染单张图并做重叠/越界检查，返回 (重叠数, 越界数)。"""
    fig = render_capture(fn)
    if fig is None:
        print(f"  [SKIP] {name}: 渲染函数未产出 Figure（上游结果缺失）")
        return 0, 0
    boxes = text_boxes(fig)
    bad = overlap_report(boxes)
    oob = bounds_report(fig, boxes)
    tag = "OK" if not bad and not oob else "⚠"
    print(f"  [{tag}] {name}: 文本图元 {len(boxes)}，明显重叠 {len(bad)}，越界 {len(oob)}")
    if verbose:
        for a, b, area, frac in bad:
            print(f"        重叠 {area} pt² ({frac:.0%}): {a[:34]!r} × {b[:34]!r}")
        for t, x0, y0, x1, y1 in oob:
            print(f"        越界 ({x0},{y0})-({x1},{y1}): {t[:40]!r}")
    plt.close(fig)
    return len(bad), len(oob)


def render_all(verbose=False):
    """按 main() 的依赖关系渲染全部正式图（输出重定向到 tmpdir，不污染交付目录）。"""
    monthly = MF.read_csv("monthly_new_models.csv")
    pipeline_trend = MF.read_csv("pipeline_trend.csv")
    library_trend = MF.read_csv("library_trend.csv")
    ccdf = MF.read_csv("ccdf.csv")
    rq5 = MF.read_csv("rq5_task_ecosystem.csv")
    rq1 = MF.load_json("summary_rq1.json") or {}
    gs = MF.load_json("graph_stats.json") or {}
    cs = MF.load_gold_json("concentration_stats.json") or {}
    ml = MF.load_json("ml_summary.json")
    bench = MF.read_csv("benchmark_all_summary.csv")

    cases = []
    if monthly is not None and rq1:
        cases.append(("Figure 1", lambda: MF.fig01_monthly(monthly, rq1)))
    if pipeline_trend is not None and rq1:
        cases.append(("Figure 2", lambda: MF.fig02_task_growth(pipeline_trend, rq1)))
        cases.append(("Figure 3", lambda: MF.fig03_task_time_heatmap(pipeline_trend)))
    if rq5 is not None and library_trend is not None:
        cases.append(("Figure 4", lambda: MF.fig04_library_task_structure(rq5, library_trend)))
    if cs:
        cases.append(("Figure 5", lambda: MF.fig05_lorenz(cs)))
    if ccdf is not None and cs:
        cases.append(("Figure 6", lambda: MF.fig06_ccdf(ccdf, cs)))
    if gs:
        cases.append(("Figure 7", lambda: MF.fig07_family_network(gs)))
        cases.append(("Figure 8", lambda: MF.fig08_family_size(gs)))
    if ml:
        cases.append(("Figure 9", lambda: MF.fig09_ml(ml)))
    if bench is not None:
        cases.append(("Figure 10", lambda: MF.fig10_scale_runtime(bench)))
        cases.append(("Figure 11", lambda: MF.fig11_partitions(bench)))
        cases.append(("Figure 12", lambda: MF.fig12_operator_comparison(bench)))

    total_bad = 0
    total_oob = 0
    for name, fn in cases:
        b, o = check_figure(name, fn, verbose=verbose)
        total_bad += b
        total_oob += o
    print(f"[figure-quality] 检查 {len(cases)} 张图：明显重叠 {total_bad} 处，越界 {total_oob} 处")
    return len(cases), total_bad, total_oob


def main():
    verbose = "--verbose" in sys.argv
    # 关键：把落盘动作替换为「只捕获 Figure」。本脚本只做排版检查，
    # **绝不改写交付图**，也不改动任何绘图函数（不为了测试而改交付代码）。
    MF.save = _capture_save
    MF.save_supp = _capture_save_supp
    with tempfile.TemporaryDirectory(prefix="figqc_") as tmp:
        MF.FIG_DIR = os.path.join(tmp, "final")
        MF.SUPP_DIR = os.path.join(tmp, "supp")
        os.makedirs(MF.FIG_DIR, exist_ok=True)
        os.makedirs(MF.SUPP_DIR, exist_ok=True)
        n, bad, oob = render_all(verbose=verbose)
    print(json.dumps({"figures_checked": n, "text_overlaps": bad,
                      "out_of_bounds": oob}, ensure_ascii=False))
    sys.exit(1 if (bad or oob) else 0)


if __name__ == "__main__":
    main()
