# 交互式模型家族探索器 · 使用说明（Task C）

> 生成时间：2026-10-13T12:01:38.312150+00:00  
> 数据快照：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`  
> 本文件由 `write_ext_docs.py` 自动生成，**所有数字取自结果文件，未手抄**。

---

## 1. 它是什么

`family_explorer.html` 是一个**离线单文件**网页（ECharts 已内嵌，无需联网、无需服务器、打开时**不运行 Spark**）。它把 RQ3 的家族结构结果变成可以**点开看**的交互视图。

入口有两个：
1. 直接双击项目根目录的 `family_explorer.html`；
2. 打开 `dashboard_final.html`，点顶部导航的「家族探索器」。

---

## 2. 能做什么

| 功能 | 说明 |
| --- | --- |
| 搜索 / 选择底座 | 按名字搜索，或从下拉列表里选一个底座（含 Top-N 家族与三个案例家族） |
| 查看直接衍生网络 | 以选中的底座为中心，展示它的直接子节点（1 跳） |
| 1 跳 / 2 跳展开 | 切换到 2 跳可看到孙节点；**案例家族才有 2 跳数据** |
| 按关系筛选 | 勾选 Finetune / Adapter / Quantized / Merge 过滤边 |
| 点节点看详情 | 显示模型名、任务类型、downloads、downloadsAllTime、created_at、关系 |
| 拖拽 / 缩放 / 重置 | 可拖动节点、滚轮缩放、点「重置视图」回到初始布局 |
| 典型家族切换 | 一键在星型 / 链型 / 合并型三个案例之间切换 |
| 节点数上限滑块 | 大图保护：超过上限的节点不会被绘制，并显示截断提示 |

---

## 3. 数据边界（**必须知道**）

页面上展示的是**有界子图**，不是全量继承网络：

| 限制项 | 当前取值 |
| --- | --- |
| 底座家族数（按直接衍生数 Top-N） | 30 |
| 每个底座的直接子节点上限 | 150 |
| 案例家族的 2 跳边上限 | 400 |
| 整页节点硬上限 | 6,000 |
| 实际节点数 | 4,908 |
| 实际边数 | 4,873 |
| 是否发生截断 | False |

本页为**代表性局部子图**：只含 Top-N 家族的直接子节点与案例家族的 2 跳，已按上限截断，**不是全量继承网络**；结论限定在显式血缘关系内。可选的时间轴仅按快照记录的 createdAt 重建，不是完整历史回放。

> 换句话说：**探索器用于「交互式理解结构」，所有量化结论仍然取自全量计算结果**，不以页面上的子图为准。

---

## 4. 生成方式（可复现）

```bash
python make_family_explorer.py
```

该脚本：读取 Silver 的 `model_base_edges` 与 `models`（用 pyarrow 按 id 过滤，不把 314 万行读进内存）→ 构造有界子图 → 把 `vendor/echarts.min.js` 与数据内嵌进 `templates/family_explorer_template.html` → 输出 `family_explorer.html` 与 `family_explorer_data.json`。

上限可用环境变量调整：`HF_EXPLORER_TOP_FAMILIES` / `HF_EXPLORER_CAP_PER_BASE` / `HF_EXPLORER_CAP_2HOP` / `HF_EXPLORER_MAX_NODES`。

---

## 5. 演示建议（答辩时）

控制在 **30 秒内**，只演示一遍：「搜索底座 → 展开 1 跳 → 按关系筛选 → 点一个节点看详情 → 切到 2 跳」；**不要**现场调上限或加载超大规模视图（会卡顿，且容易被问「这是全量吗」）。被问到时直接回答：**这是有界子图，页面有截断提示，量化结论取自全量计算**。

---

## 6. 产出文件

| 文件 | 内容 |
| --- | --- |
| `family_explorer.html` | 离线单文件交互页面（ECharts + 数据内嵌） |
| `data_hubstats/gold/analysis/family_explorer_data.json` | 有界子图数据（可复用） |
| `vendor/echarts.min.js` | ECharts 5.5.1（Apache-2.0，本地内嵌） |
| `templates/family_explorer_template.html` | 页面模板（占位符替换） |

