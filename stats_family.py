#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stats_family.py — 模型家族结构分析的**纯算法层**（增量完善 Task A/D）。

只放不依赖 Spark / pandas 的纯计算，便于随时快速回归（与 stats_utils.py 同样的定位）。
被 analysis_family_ext.py / analysis_concentration_groups.py 复用，也被 test_family_ext.py 单测。

为什么把「环」单列出来：
    model_base_edges 是**有向图**，且实测存在自环（src == dst）与可能的更大多父/互指结构。
    「最长衍生链」这类指标在**含环图**上无定义（最长的简单路径是 NP-hard），
    因此本模块先在图上求**强连通分量（SCC）**，把环折叠成超级节点，
    再在**凝聚 DAG** 上求最长路径 —— 这样深度指标既严格定义、又保留真实拓扑。

命名纪律：本模块所有"深度"均指**凝聚 DAG 上从任一源点出发的最长路径长度**，
不等同于"树的层数"，也不等同于可达后代数。
"""
import math


def build_index(node_ids):
    """字符串 id -> 连续整数下标。返回 (index_map, ordered_ids)。"""
    index_map = {}
    ordered = []
    for nid in node_ids:
        if nid not in index_map:
            index_map[nid] = len(ordered)
            ordered.append(nid)
    return index_map, ordered


def adjacency_from_edges(n, edge_pairs_idx):
    """由整数边对构造出邻接表（去重、去自环留给调用方决定）。"""
    adj = [set() for _ in range(n)]
    for a, b in edge_pairs_idx:
        adj[a].add(b)
    return [sorted(s) for s in adj]


def strongly_connected_components(n, adj):
    """迭代版 Tarjan SCC，返回分量列表（每个分量是节点下标列表）。

    迭代实现（不递归）以避免 93 万节点上的 Python 递归深度上限。
    含自环的节点会自成一个分量（size==1），调用方据 n_self_loop 另行判断。
    """
    index = [-1] * n
    low = [0] * n
    on_stack = [False] * n
    stack = []
    comps = []
    counter = 0
    for root in range(n):
        if index[root] != -1:
            continue
        work = [(root, 0)]
        while work:
            v, pi = work[-1]
            if pi == 0:
                index[v] = low[v] = counter
                counter += 1
                stack.append(v)
                on_stack[v] = True
            recurse = False
            neighbors = adj[v]
            i = pi
            while i < len(neighbors):
                w = neighbors[i]
                if index[w] == -1:
                    work[-1] = (v, i + 1)
                    work.append((w, 0))
                    recurse = True
                    break
                if on_stack[w] and index[w] < low[v]:
                    low[v] = index[w]
                i += 1
            if recurse:
                continue
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    comp.append(w)
                    if w == v:
                        break
                comps.append(comp)
            work.pop()
            if work:
                p = work[-1][0]
                if low[v] < low[p]:
                    low[p] = low[v]
    return comps


def component_of(n, comps):
    """节点下标 -> 分量下标。"""
    comp_of = [0] * n
    for cid, comp in enumerate(comps):
        for v in comp:
            comp_of[v] = cid
    return comp_of


def condensation_edges(edge_pairs_idx, comp_of, n_comp):
    """凝聚 DAG 的边（去掉分量内边与自环后的去重边集）。"""
    out = set()
    for a, b in edge_pairs_idx:
        ca, cb = comp_of[a], comp_of[b]
        if ca != cb:
            out.add((ca, cb))
    return out


def condensation_adjacency(n_comp, cond_edges):
    adj = [[] for _ in range(n_comp)]
    for ca, cb in cond_edges:
        adj[ca].append(cb)
    return adj


def topological_order(n, adj):
    """Kahn 拓扑排序（要求 DAG）。返回 (order, ok)；有环则 ok=False。"""
    indeg = [0] * n
    for v in range(n):
        for w in adj[v]:
            indeg[w] += 1
    queue = [v for v in range(n) if indeg[v] == 0]
    order = []
    head = 0
    while head < len(queue):
        v = queue[head]
        head += 1
        order.append(v)
        for w in adj[v]:
            indeg[w] -= 1
            if indeg[w] == 0:
                queue.append(w)
    return order, len(order) == n


def longest_path_depth(n, adj):
    """凝聚 DAG 上每个分量的深度 = 从任一源点出发到它的最长路径长度（边数）。

    depth(源)=0；depth(v)=max(depth(u)+1) over 前驱 u。
    这是 DAG 上的最长路径，**不是**树的层数，也不是可达后代数。
    """
    order, ok = topological_order(n, adj)
    if not ok:
        raise ValueError("longest_path_depth 只适用于 DAG；请先做 SCC 凝聚")
    depth = [0] * n
    for v in order:
        for w in adj[v]:
            if depth[v] + 1 > depth[w]:
                depth[w] = depth[v] + 1
    return depth


def longest_path_height(n, adj):
    """凝聚 DAG 上每个分量的**子树高度** = 从它出发到任一汇点的最长路径长度（边数）。

    与 longest_path_depth 互补：
      depth  描述"离根多远"（从源点到它的最长路径）；
      height 描述"往下还有多深"（它到可达汇点的最长路径）——即"最长衍生链"的度量。
    反向拓扑序 DP，O(V+E)。这是 DAG 上的最长路径，**不是**可达后代数。
    """
    order, ok = topological_order(n, adj)
    if not ok:
        raise ValueError("longest_path_height 只适用于 DAG；请先做 SCC 凝聚")
    height = [0] * n
    for v in reversed(order):
        best = 0
        for w in adj[v]:
            if height[w] + 1 > best:
                best = height[w] + 1
        height[v] = best
    return height


def theil_t(values):
    """Theil T 指数（正数集合）：T = (1/n) Σ (x_i/μ)·ln(x_i/μ)。

    仅对 x_i > 0 有定义；含 0 时应先取正子集并另行报告零值占比。
    """
    xs = [float(v) for v in values if v > 0]
    n = len(xs)
    if n == 0:
        return None
    mean = sum(xs) / n
    if mean <= 0:
        return None
    total = 0.0
    for x in xs:
        r = x / mean
        total += r * math.log(r)
    return total / n


def theil_decomposition(values, group_keys):
    """Theil T 的组间/组内分解。

    输入：values 与 group_keys 等长；只使用 value>0 的样本（返回 n_used / n_zero）。
    T_total = Σ_g s_g·T_g  +  Σ_g s_g·ln(μ_g/μ)
               └ 组内贡献 T_within ┘   └ 组间贡献 T_between ┘
    其中 s_g = 组 g 的取值和 / 总和，μ_g 组均值，μ 总均值。
    """
    pairs = [(float(v), k) for v, k in zip(values, group_keys)]
    n_zero = sum(1 for v, _ in pairs if v <= 0)
    pairs = [(v, k) for v, k in pairs if v > 0]
    n = len(pairs)
    if n == 0:
        return {"n_used": 0, "n_zero": n_zero, "theil_total": None,
                "theil_within": None, "theil_between": None,
                "between_share": None, "groups": []}
    grand_sum = sum(v for v, _ in pairs)
    grand_mean = grand_sum / n
    groups = {}
    for v, k in pairs:
        groups.setdefault(k, []).append(v)
    within = 0.0
    between = 0.0
    detail = []
    for k, vs in groups.items():
        s_g = sum(vs) / grand_sum
        mu_g = sum(vs) / len(vs)
        t_g = 0.0
        for x in vs:
            r = x / mu_g
            t_g += r * math.log(r)
        t_g /= len(vs)
        within += s_g * t_g
        between += s_g * math.log(mu_g / grand_mean)
        detail.append({"group": k, "n": len(vs), "sum": sum(vs),
                       "share_of_total": s_g, "mean": mu_g, "theil_within": t_g})
    total = within + between
    detail.sort(key=lambda d: -d["sum"])
    return {
        "n_used": n, "n_zero": n_zero,
        "theil_total": total, "theil_within": within, "theil_between": between,
        "between_share": (between / total) if total > 0 else None,
        "groups": detail,
    }


def theil_t_from_freq(values, freqs):
    """频数表上的 Theil T：T = (Σ x·ln x)/(n·μ) − ln μ，只用正取值。

    与 theil_t 等价但不必展开成逐行列表，可在 314 万行规模上安全计算。
    返回 None 表示正样本为空。
    """
    n = 0
    s = 0.0
    sxlnx = 0.0
    for v, f in zip(values, freqs):
        if v > 0:
            n += int(f)
            s += float(v) * int(f)
            sxlnx += int(f) * float(v) * math.log(float(v))
    if n == 0 or s <= 0:
        return None
    mu = s / n
    return sxlnx / (n * mu) - math.log(mu)


def theil_decomposition_from_freq(group_freqs):
    """频数表上的 Theil 组间/组内分解。

    group_freqs: {group_key: (values, freqs)}。
    T_total = Σ_g s_g·T_g + Σ_g s_g·ln(μ_g/μ)，s_g = 组取值和/总和。
    """
    grand_n = 0
    grand_sum = 0.0
    per = []
    for key, (vals, freqs) in group_freqs.items():
        n = 0
        s = 0.0
        sxlnx = 0.0
        n_zero = 0
        for v, f in zip(vals, freqs):
            f = int(f)
            if v > 0:
                n += f
                s += float(v) * f
                sxlnx += f * float(v) * math.log(float(v))
            else:
                n_zero += f
        grand_n += n
        grand_sum += s
        per.append({"group": key, "n": n, "n_zero": n_zero, "sum": s, "sxlnx": sxlnx})
    if grand_n == 0 or grand_sum <= 0:
        return {"theil_total": None, "theil_within": None, "theil_between": None,
                "between_share": None, "groups": []}
    grand_mean = grand_sum / grand_n
    within = 0.0
    between = 0.0
    detail = []
    for p in per:
        if p["n"] == 0 or p["sum"] <= 0:
            continue
        mu_g = p["sum"] / p["n"]
        t_g = p["sxlnx"] / (p["n"] * mu_g) - math.log(mu_g)
        s_g = p["sum"] / grand_sum
        within += s_g * t_g
        between += s_g * math.log(mu_g / grand_mean)
        detail.append({"group": p["group"], "n": p["n"], "n_zero": p["n_zero"],
                       "sum": p["sum"], "mean": mu_g, "share_of_total": s_g,
                       "theil_within": t_g})
    total = within + between
    detail.sort(key=lambda d: -d["sum"])
    return {"theil_total": total, "theil_within": within, "theil_between": between,
            "between_share": (between / total) if total > 0 else None, "groups": detail}


def classify_structure(n_children, subtree_height, leaf_child_share, merge_share,
                       thresholds=None):
    """可复现的家族结构判定：star / chain / merge / mixed。

    判定优先级与阈值（全部显式，便于复现与质疑）。指标定义：
      n_children        底座被多少模型**直接**继承（出度）；
      subtree_height    底座在凝聚 DAG 上的**最长衍生链**（边数，见 longest_path_height）；
      leaf_child_share  直接子节点中自身无子节点的比例（宽而浅时接近 1）；
      merge_share       底座出边中 relation=merge 的比例。

    规则（先命中先返回）：
      1) merge：merge_share >= 0.20（该底座的直接继承以"合并"为主）；
      2) chain：subtree_height >= 6 且 leaf_child_share >= 0.60（又深又窄的长链）；
      3) star ：n_children >= 100 且 subtree_height <= 6 且 leaf_child_share >= 0.50
                （一中心放射、几乎无二级：如 FLUX.1-dev）；
      4) 其余 mixed。

    说明：**不**用"家族内存在多父节点"作为 merge 判据 —— 实测 88,333 个底座里
    10,147 个至少有一个多父子节点，该判据几乎无法区分结构，故仅作为报告指标保留。
    """
    t = thresholds or {"merge_share": 0.20, "chain_height": 6, "chain_leaf": 0.60,
                       "star_children": 100, "star_height": 6, "star_leaf": 0.50}
    reasons = []
    if merge_share >= t["merge_share"]:
        reasons.append(f"merge_share={merge_share:.3f} >= {t['merge_share']}")
        return "merge", reasons
    if subtree_height >= t["chain_height"] and leaf_child_share >= t["chain_leaf"]:
        reasons.append(f"height={subtree_height} >= {t['chain_height']} 且 "
                       f"leaf_share={leaf_child_share:.3f} >= {t['chain_leaf']}")
        return "chain", reasons
    if (n_children >= t["star_children"] and subtree_height <= t["star_height"]
            and leaf_child_share >= t["star_leaf"]):
        reasons.append(f"children={n_children} >= {t['star_children']}, "
                       f"height={subtree_height} <= {t['star_height']}, "
                       f"leaf_share={leaf_child_share:.3f} >= {t['star_leaf']}")
        return "star", reasons
    reasons.append(f"children={n_children}, height={subtree_height}, "
                   f"leaf_share={leaf_child_share:.3f}, merge_share={merge_share:.3f} 未命中阈值")
    return "mixed", reasons
