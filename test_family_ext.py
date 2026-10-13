#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_family_ext.py — 增量完善 Task A/D 的**纯算法单元测试**（不需要 Spark）。

覆盖：
  1. 迭代 Tarjan SCC 在含环、自环、多分量图上的正确性；
  2. 凝聚图必为 DAG（拓扑排序成功），最长路径深度值正确；
  3. 结构判定 classify_structure 的星/链/合并/混合四类；
  4. Theil T 的解析值与组间/组内分解恒等式 T_total == T_within + T_between。

运行：python test_family_ext.py
"""
import math
import sys

import stats_family as SF

FAILURES = []


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAILURES.append(name)


def make_graph(edges):
    """由 (src,dst) 字符串边构造邻接表，返回 (n, adj, index_map)。"""
    ids = []
    for a, b in edges:
        ids.append(a)
        ids.append(b)
    index_map, ordered = SF.build_index(ids)
    n = len(ordered)
    pairs = [(index_map[a], index_map[b]) for a, b in edges]
    adj = SF.adjacency_from_edges(n, pairs)
    return n, adj, index_map


def test_scc_cycle():
    n, adj, idx = make_graph([("0", "1"), ("1", "2"), ("2", "0"), ("3", "0"), ("2", "4")])
    comps = SF.strongly_connected_components(n, adj)
    comp_sets = sorted(sorted(idx_name for idx_name in [c]) for c in comps)
    # 期望 {0,1,2} 同一分量，3 与 4 各自单独
    got = sorted(sorted(c) for c in comps)
    sizes = sorted(len(c) for c in comps)
    check("SCC：3-环 {0,1,2} 合并为一个分量", sizes == [1, 1, 3], f"sizes={sizes}")
    comp_of = SF.component_of(n, comps)
    check("SCC：0/1/2 属于同一分量",
          comp_of[idx["0"]] == comp_of[idx["1"]] == comp_of[idx["2"]])
    check("SCC：3 与 0 不在同一分量", comp_of[idx["3"]] != comp_of[idx["0"]])


def test_scc_self_loop():
    n, adj, idx = make_graph([("a", "a"), ("a", "b")])
    comps = SF.strongly_connected_components(n, adj)
    comp_of = SF.component_of(n, comps)
    check("SCC：自环节点 a 自成一个 size=1 分量",
          sum(1 for c in comps if len(c) == 1) == 2, f"n_comp={len(comps)}")
    check("SCC：a 与 b 不同分量", comp_of[idx["a"]] != comp_of[idx["b"]])


def test_condensation_dag_and_depth():
    # 0->1->2->0 (环) ; 2->3 ; 3->4 ; 3->5
    n, adj, idx = make_graph([("0", "1"), ("1", "2"), ("2", "0"),
                              ("2", "3"), ("3", "4"), ("3", "5")])
    comps = SF.strongly_connected_components(n, adj)
    comp_of = SF.component_of(n, comps)
    nc = len(comps)
    pairs = [(idx["0"], idx["1"]), (idx["1"], idx["2"]), (idx["2"], idx["0"]),
             (idx["2"], idx["3"]), (idx["3"], idx["4"]), (idx["3"], idx["5"])]
    ce = SF.condensation_edges(pairs, comp_of, nc)
    cadj = SF.condensation_adjacency(nc, ce)
    order, ok = SF.topological_order(nc, cadj)
    check("凝聚图是 DAG（拓扑排序成功）", ok, f"n_comp={nc}")
    depth = SF.longest_path_depth(nc, cadj)
    # 环分量 depth=0；3 的 depth=1；4/5 的 depth=2
    check("凝聚图深度：环分量=0", depth[comp_of[idx["0"]]] == 0)
    check("凝聚图深度：节点3=1", depth[comp_of[idx["3"]]] == 1)
    check("凝聚图深度：叶子4/5=2",
          depth[comp_of[idx["4"]]] == 2 and depth[comp_of[idx["5"]]] == 2)


def test_longest_path_chain():
    # 纯链 0->1->2->3->4
    n, adj, idx = make_graph([("0", "1"), ("1", "2"), ("2", "3"), ("3", "4")])
    depth = SF.longest_path_depth(n, adj)
    check("最长路径：链式深度 0..4",
          all(depth[idx[str(i)]] == i for i in range(5)))


def test_longest_path_height():
    n, adj, idx = make_graph([("0", "1"), ("1", "2"), ("2", "3"), ("3", "4")])
    h = SF.longest_path_height(n, adj)
    check("子树高度：链式 height 4..0",
          all(h[idx[str(i)]] == 4 - i for i in range(5)), f"{[h[idx[str(i)]] for i in range(5)]}")
    # 分叉：0->1,0->2,1->3,2->3,3->4  -> height(0)=3, height(3)=1
    n2, adj2, idx2 = make_graph([("0", "1"), ("0", "2"), ("1", "3"), ("2", "3"), ("3", "4")])
    h2 = SF.longest_path_height(n2, adj2)
    check("子树高度：分叉图 height(0)=3, height(3)=1",
          h2[idx2["0"]] == 3 and h2[idx2["3"]] == 1)


def test_classify_structure():
    lab, _ = SF.classify_structure(200, 3, 0.95, 0.0)
    check("结构判定：星型（宽而浅）", lab == "star", lab)
    lab, _ = SF.classify_structure(5, 8, 0.8, 0.0)
    check("结构判定：链型（深而窄）", lab == "chain", lab)
    lab, _ = SF.classify_structure(50, 3, 0.5, 0.30)
    check("结构判定：合并型（merge_share 命中）", lab == "merge", lab)
    lab, _ = SF.classify_structure(10, 3, 0.5, 0.05)
    check("结构判定：混合型", lab == "mixed", lab)
    # 高 merge_share 优先于 star/chain
    lab, _ = SF.classify_structure(5000, 2, 0.99, 0.50)
    check("结构判定：merge 优先于 star", lab == "merge", lab)
    # FLUX.1-dev 的真实指标应判为 star（children 43419, height 5, leaf 0.998, merge 0.0002）
    lab, _ = SF.classify_structure(43419, 5, 0.998, 0.0002)
    check("结构判定：FLUX.1-dev 指标 -> star", lab == "star", lab)


def test_theil():
    check("Theil：全等值 T=0", abs(SF.theil_t([5, 5, 5, 5])) < 1e-12)
    check("Theil：空/全零返回 None", SF.theil_t([0, 0]) is None)
    # 单组：组内 == 总，组间 == 0
    d = SF.theil_decomposition([1, 2, 4, 8], ["A", "A", "A", "A"])
    check("Theil 单组：between=0", abs(d["theil_between"]) < 1e-12, str(d["theil_between"]))
    check("Theil 单组：total==within",
          abs(d["theil_total"] - d["theil_within"]) < 1e-12)
    # 分解恒等式：随机多组
    import random
    rng = random.Random(2026)
    ok = True
    worst = 0.0
    for _ in range(200):
        n = rng.randint(2, 60)
        vals = [rng.randint(1, 500) for _ in range(n)]
        keys = [rng.choice(["A", "B", "C", "D"]) for _ in range(n)]
        dd = SF.theil_decomposition(vals, keys)
        worst = max(worst, abs(dd["theil_total"] - (dd["theil_within"] + dd["theil_between"])))
    check("Theil 分解恒等式 T_total == T_within + T_between（200 组）",
          worst < 1e-9, f"max|Δ|={worst:.2e}")
    # 组间完全分离：两组常量且均值不同 -> 组内=0
    d = SF.theil_decomposition([2, 2, 2, 8, 8, 8], ["A", "A", "A", "B", "B", "B"])
    check("Theil 两组常量：组内=0", abs(d["theil_within"]) < 1e-12, str(d["theil_within"]))
    check("Theil 两组常量：组间>0", d["theil_between"] > 0)


def test_theil_from_freq():
    # 与逐行版等价：[1,1,4]
    got = SF.theil_t_from_freq([1, 4], [2, 1])
    check("Theil 频数版 == 解析值 ln2/3", abs(got - math.log(2) / 3) < 1e-12, f"{got:.12f}")
    # 频数版 vs 逐行版随机对拍
    import random
    rng = random.Random(7)
    worst = 0.0
    for _ in range(300):
        n = rng.randint(2, 200)
        vals = [rng.randint(0, 500) for _ in range(n)]
        uniq = sorted(set(vals))
        freqs = [vals.count(u) for u in uniq]
        a = SF.theil_t(vals)
        b = SF.theil_t_from_freq(uniq, freqs)
        if a is None or b is None:
            if a is not None or b is not None:
                worst = 1e9
            continue
        worst = max(worst, abs(a - b))
    check("Theil 频数版 == 逐行版（300 组随机）", worst < 1e-9, f"max|Δ|={worst:.2e}")
    # 分解恒等式（频数版）
    gf = {"A": ([2], [3]), "B": ([8], [3])}
    d = SF.theil_decomposition_from_freq(gf)
    check("Theil 频数分解：组内=0（两组常量）", abs(d["theil_within"]) < 1e-12)
    check("Theil 频数分解恒等式",
          abs(d["theil_total"] - (d["theil_within"] + d["theil_between"])) < 1e-12)


def test_theil_analytic():
    # 手工解析：values [1,1,4], 单组 -> T = (1/3)[ (1/2)ln(1/2)*2 + 2*ln2 ] = (1/3)[-ln2 + 2ln2]=ln2/3
    d = SF.theil_decomposition([1, 1, 4], ["G", "G", "G"])
    want = math.log(2) / 3
    check("Theil 解析值（[1,1,4] 单组）", abs(d["theil_total"] - want) < 1e-12,
          f"got={d['theil_total']:.12f} want={want:.12f}")


def main():
    test_scc_cycle()
    test_scc_self_loop()
    test_condensation_dag_and_depth()
    test_longest_path_chain()
    test_longest_path_height()
    test_classify_structure()
    test_theil()
    test_theil_from_freq()
    test_theil_analytic()
    print()
    if FAILURES:
        print(f"FAILED {len(FAILURES)} 项: {FAILURES}")
        sys.exit(1)
    print("全部家族结构算法测试通过")


if __name__ == "__main__":
    main()
