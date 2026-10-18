# -*- coding: utf-8 -*-
"""重做两张结构图，目标：字号够大、颜色够深，投影时能看清。

输出：
    架构图_三层数据流转.png / .svg
    环境层次图_运行环境.png / .svg

运行：python make_structure_figs.py
"""
import os

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

HERE = os.path.dirname(os.path.abspath(__file__))

FONT_REG = r"C:\Windows\Fonts\msyh.ttc"
FONT_BOLD = r"C:\Windows\Fonts\msyhbd.ttc"
FONT_MONO = r"C:\Windows\Fonts\consola.ttf"
FONT_MONO_B = r"C:\Windows\Fonts\consolab.ttf"
if not (os.path.exists(FONT_MONO) and os.path.exists(FONT_MONO_B)):
    FONT_MONO = FONT_MONO_B = FONT_REG

PAGE_BG = "#EDF1F8"
CARD_BG = "#FFFFFF"
CARD_EDGE = "#C4D2E5"

INK = "#0E1A2B"      # 标题：近黑
INK2 = "#1E3557"     # 正文：深蓝灰（比原来明显加深）
SUB = "#3A5477"      # 次级说明：比原来的浅灰明显加深

C_BLUE = "#1A5EA6"
C_TEAL = "#15705D"
C_ORANGE = "#A8471A"
C_ARROW = "#4F7099"

XMAX, YMAX = 160.0, 90.0


def fp(size, bold=False):
    return FontProperties(fname=FONT_BOLD if bold else FONT_REG, size=size)


def canvas():
    fig = plt.figure(figsize=(16, 9), dpi=120)
    fig.patch.set_facecolor(PAGE_BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, XMAX)
    ax.set_ylim(0, YMAX)
    ax.axis("off")
    return fig, ax


def card(ax, x, y, w, h, face=CARD_BG, edge=CARD_EDGE, lw=1.5, r=2.0, z=2):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
        linewidth=lw, edgecolor=edge, facecolor=face, zorder=z))


def txt(ax, x, y, s, size, color=INK2, bold=False, ha="left", va="center", z=10):
    ax.text(x, y, s, color=color, fontproperties=fp(size, bold),
            ha=ha, va=va, zorder=z)


def save(fig, name):
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(HERE, f"{name}.{ext}"),
                    facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"saved: {name}.png / {name}.svg")


# ---------------------------------------------------------------- 架构图
def fig_architecture():
    fig, ax = canvas()
    txt(ax, 80, 85.5, "数据分层与流转", 36, INK, True, "center")
    txt(ax, 80, 79.6, "源：cfahlgren1/hub-stats · models.parquet @ 2c1e1950",
        16, SUB, False, "center")

    W, H, TOP = 47.0, 50.0, 73.0
    xs = [3.0, 56.5, 110.0]
    blocks = [
        ("BRONZE · 原始层", C_BLUE, "1 张宽表 · 3,138,959 行 · 1.49 GB", [
            "20 个顶层字段（69 个叶子列）",
            "含嵌套结构与数组",
            "原样保存，一个字节不改",
        ]),
        ("SILVER · 明细层", C_TEAL, "3 张表", [
            "models · 16 列 · 3,138,954 行",
            "model_tags · 2 列 · 20,625,635 行",
            "model_base_edges · 3 列 · 919,839 行",
        ]),
        ("GOLD · 结果层", C_ORANGE, "1 张宽表 + 7 张统计表", [
            "宽表：16 个基础列 + 13 个派生列",
            "月度表 · 下载分布 · 生命周期",
            "家族规模 · 建模特征表 · 曲线点",
        ]),
    ]

    for x, (name, color, sub, lines) in zip(xs, blocks):
        card(ax, x, TOP - H, W, H)
        txt(ax, x + 3.0, TOP - 7.0, name, 22, color, True)
        txt(ax, x + 3.0, TOP - 15.5, sub, 16, SUB)
        ax.plot([x + 3.0, x + W - 3.0], [TOP - 20.5, TOP - 20.5],
                color="#D5E0EF", lw=1.3, zorder=6)
        for i, ln in enumerate(lines):
            txt(ax, x + 3.0, TOP - 28.0 - i * 10.0, ln, 18, INK2)

    for x0, x1 in ((50.0, 56.5), (103.5, 110.0)):
        ax.add_patch(FancyArrowPatch(
            (x0, 44.0), (x1, 44.0), arrowstyle="-|>", mutation_scale=30,
            linewidth=3.8, color="#3F5F8A", zorder=8))

    txt(ax, 80, 13.0, "原始层只读　→　明细层规范化　→　结果层出结论",
        18, SUB, False, "center")
    save(fig, "架构图_三层数据流转")


# ---------------------------------------------------------------- 环境图
def fig_runtime():
    fig, ax = canvas()
    txt(ax, 80, 85.5, "项目运行环境", 36, INK, True, "center")

    card(ax, 4.5, 4.5, 151.0, 76.0, face="#F8FBFF", edge="#B9CBE4", lw=1.8, r=2.2)
    txt(ax, 8.5, 76.0, "Windows 11 操作系统", 20, INK2, True)

    # ---- 左：JVM ----
    card(ax, 9.5, 18.0, 67.0, 54.0, face="#E8F1FC", edge=C_BLUE, lw=1.8)
    txt(ax, 12.5, 68.5, "JVM 进程", 21, C_BLUE, True)
    txt(ax, 12.5, 62.5, "Java 运行环境：OpenJDK 17", 16, INK2)

    card(ax, 12.5, 19.5, 61.0, 38.5, face="#FFFFFF", edge="#9FBCE0", lw=1.5)
    txt(ax, 15.5, 54.5, "Spark 引擎", 19, C_BLUE, True)
    txt(ax, 15.5, 49.0, "PySpark 4.2.0", 16, INK2)

    card(ax, 15.5, 21.5, 55.0, 25.0, face="#E7F5EF", edge=C_TEAL, lw=1.5)
    txt(ax, 18.0, 43.5, "扩展库与额外 jar", 16.5, C_TEAL, True)
    for i, ln in enumerate(["GraphFrames 0.12.3",
                            "└ 手工装进 jars 目录",
                            "hadoop-bare-naked-local-fs",
                            "└ 绕开 Windows 权限限制",
                            "MLlib　·　GraphX"]):
        txt(ax, 18.0, 39.3 - i * 3.5, ln, 14.5, INK2)

    # ---- 右：Python ----
    card(ax, 83.5, 18.0, 67.0, 54.0, face="#FDEEE9", edge="#C0503A", lw=1.8)
    txt(ax, 86.5, 68.5, "Python 进程", 21, "#B03A22", True)
    txt(ax, 86.5, 62.5, "conda 环境 hf_eco · Python 3.11", 16, INK2)

    card(ax, 86.5, 21.5, 61.0, 36.5, face="#FFFFFF", edge="#DDA593", lw=1.5)
    txt(ax, 89.5, 54.5, "我们写的脚本", 19, "#B03A22", True)
    txt(ax, 89.5, 49.0, "两条路径共用同一套口径", 16, INK2)

    card(ax, 89.5, 24.0, 55.0, 21.5, face="#FBF4F1", edge="#E0B6A8", lw=1.5)
    txt(ax, 92.0, 42.5, "分四类", 16.5, "#B03A22", True)
    for i, ln in enumerate(["ETL：清洗与三层落地",
                            "分析：RQ1–RQ5 与补充",
                            "图与机器学习：JVM 引擎",
                            "出图与校验：214 项检查"]):
        txt(ax, 92.0, 38.3 - i * 4.0, ln, 15.5, INK2)

    # ---- 中间：py4j ----
    ax.add_patch(FancyArrowPatch(
        (77.0, 45.0), (83.0, 45.0), arrowstyle="<|-|>", mutation_scale=24,
        linewidth=3.0, color="#7A4FB5", zorder=8))
    txt(ax, 80.0, 52.5, "py4j", 17, "#6B3FA8", True, "center")

    # ---- 底部：运行参数 ----
    card(ax, 9.5, 7.0, 141.0, 8.0, face="#FFFFFF", edge="#C4D2E5", lw=1.4, r=1.6)
    txt(ax, 13.0, 11.0,
        "运行参数：单机 local[6]　·　driver 内存 3 GB　·　shuffle 分区 64　·　会话时区 UTC",
        15.5, INK2)
    save(fig, "环境层次图_运行环境")


if __name__ == "__main__":
    fig_architecture()
    fig_runtime()
