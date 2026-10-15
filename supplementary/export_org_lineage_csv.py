# -*- coding: utf-8 -*-
"""把 ①② 的结果 JSON 展平成 CSV（目标要求"结果 JSON/CSV"，此前只有 JSON）。

术语：分组字段是 `author`，即**发布账号**（同时包含个人账号与机构账号）。
CSV 列名沿用中性英文字段（`account` = 发布账号），中文表述见
`org_lineage_supplementary.md`；图表内文字直接写「发布账号」。

用法：python export_org_lineage_csv.py
产出：org_concentration_summary.csv / top20_orgs_by_downloads.csv /
      top20_orgs_by_n_models.csv / lineage_family_top20.csv / lineage_head_to_head.csv
"""
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# account -> 发布账号；org -> 官方发布账号前缀
FIELD_ORDER = {
    "org_concentration_summary.csv": ["metric", "value"],
    "top20_orgs_by_downloads.csv": ["account", "n_models", "downloads",
                                    "downloadsAllTime", "tag_mean"],
    "top20_orgs_by_n_models.csv": ["account", "n_models", "downloads"],
    "lineage_family_top20.csv": ["family", "n_edges", "n_active_bases",
                                 "edges_per_active_base", "n_models_in_snapshot",
                                 "share_of_all_edges"],
    "lineage_head_to_head.csv": ["label", "org", "n_edges", "n_active_bases",
                                 "edges_per_active_base", "n_models_in_snapshot",
                                 "share_of_all_edges"],
}


def load(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as f:
        return json.load(f)


def write_csv(name, rows, fields=None):
    p = os.path.join(HERE, name)
    if not rows:
        print(f"  (跳过 {name}：无数据)")
        return
    fields = fields or FIELD_ORDER.get(name) or list(rows[0].keys())
    # 只保留实际存在的列，避免缺列报错
    fields = [c for c in fields if any(c in r for r in rows)]
    with open(p, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {name}  ({len(rows)} 行)")


def main():
    org = load("org_concentration_result.json")
    lin = load("lineage_family_activity_result.json")

    # ① 标量指标
    scal = [{"metric": k, "value": v} for k, v in org.items()
            if not isinstance(v, (dict, list))]
    scal += [{"metric": f"theil.{k}", "value": v} for k, v in org["theil"].items()]
    scal += [{"metric": f"tag_count_proxy.{k}", "value": v}
             for k, v in org["tag_count_proxy"].items()]
    scal += [{"metric": f"config.{k}", "value": v} for k, v in lin["config"].items()
             if not isinstance(v, list)]
    scal += [{"metric": f"lineage.{k}", "value": v} for k, v in lin.items()
             if not isinstance(v, (dict, list))]
    write_csv("org_concentration_summary.csv", scal, ["metric", "value"])

    write_csv("top20_orgs_by_downloads.csv", org["top20_orgs_by_downloads"])
    write_csv("top20_orgs_by_n_models.csv", org["top20_orgs_by_n_models"])
    write_csv("lineage_family_top20.csv", lin["family_activity_top20"])
    write_csv("lineage_head_to_head.csv",
              [{"label": k, **v} for k, v in lin["head_to_head"].items()])


if __name__ == "__main__":
    main()
