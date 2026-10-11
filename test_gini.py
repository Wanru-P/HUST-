#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_gini.py — 验证「升序秩公式」与「累计和公式」在数学上等价（含大量零值的情形）。

背景（Phase 0 计划曾怀疑两式在零值下不一致，Phase 1A 要求先用已知数组验证）：
  A. 升序秩公式：  G = 2*S/(n*total) - 1,  S = Σ (rank - 0.5) * x,  rank=1..n（升序）
  B. 累计和公式：  G = (n + 1 - 2 * Σ cum_i / total) / n,  cum_i = Σ_{j<=i} x_(j)（升序）

推导：Σ cum_i = Σ (n+1-j) x_j = (n+1)*total - Σ j*x_j
      => B = (n+1)/n - 2[(n+1)total - Σ j x_j]/(n total)
           = 2Σ j x_j/(n total) - (n+1)/n
      而 A = 2[Σ j x_j - 0.5 total]/(n total) - 1 = 2Σ j x_j/(n total) - (n+1)/n
      两式恒等。

本脚本用真实数组（含全零、含零混合、均匀、随机）实测验证。
"""
import json
import sys

import numpy as np


def gini_rank_formula(x):
    """升序秩公式（analysis_evo.py / analysis_rq5.py 口径）。"""
    x = np.sort(np.asarray(x, dtype=float))
    n = len(x)
    total = x.sum()
    if n == 0 or total == 0:
        return 0.0
    rank = np.arange(1, n + 1, dtype=float)
    s = float(((rank - 0.5) * x).sum())
    return 2.0 * s / (n * total) - 1.0


def gini_cumsum_formula(x):
    """累计和公式（local_analysis.py 口径）。"""
    x = np.sort(np.asarray(x, dtype=float))
    n = len(x)
    total = x.sum()
    if n == 0 or total == 0:
        return 0.0
    cum = np.cumsum(x)
    return float((n + 1 - 2.0 * (cum / total).sum()) / n)


CASES = {
    "classic_[0,10]": [0, 10],
    "zeros_[0,0,1]": [0, 0, 1],
    "simple_[1,2,3]": [1, 2, 3],
    "mixed_[0,0,0,5,5]": [0, 0, 0, 5, 5],
    "all_equal_[1]*10": [1] * 10,
    "all_zero_[0]*10": [0] * 10,
    "heavy_tail_[0]*90+[1]+[1000]": [0] * 90 + [1, 1000],
    "lorenz_extreme_[0]*99+[100]": [0] * 99 + [100],
}


def main():
    rng = np.random.default_rng(42)
    cases = dict(CASES)
    cases["random_uniform_100"] = list(rng.random(100))
    cases["random_longtail_200"] = list(rng.pareto(1.5, 200))

    rows = []
    max_diff = 0.0
    for name, arr in cases.items():
        a = gini_rank_formula(arr)
        b = gini_cumsum_formula(arr)
        d = abs(a - b)
        max_diff = max(max_diff, d)
        rows.append({"case": name, "n": len(arr),
                     "gini_rank": round(a, 12), "gini_cumsum": round(b, 12),
                     "abs_diff": d})
        print(f"{name:34s} n={len(arr):4d}  rank={a:.10f}  cumsum={b:.10f}  diff={d:.2e}")

    print(f"\n最大绝对差 = {max_diff:.3e}")
    ok = max_diff < 1e-9
    print("[OK] 两式在全部测试数组上等价（差异在浮点误差内）" if ok
          else "[FAIL] 两式不一致，需重新检查公式")
    out = {"max_abs_diff": max_diff, "equivalent": bool(ok), "cases": rows}
    with open("gini_equivalence_result.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
