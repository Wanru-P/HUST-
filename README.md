# 开源人工智能模型生态的演化、注意力集中与模型谱系分析

《大数据导论》课程项目。以 Hugging Face Hub 的公开模型快照为对象，使用 PySpark 对开源模型生态的时间演化、下载注意力集中、模型谱系网络及任务生态差异进行可复现分析。

## 本仓库包含什么

- `report_final.md`：最终研究报告。
- `figures_final/`：12 张正式图（PNG 与 SVG）。
- `dashboard_final.html`、`dashboard_standalone.html`：无需运行 Spark 的离线结果看板；后者可单文件分享。
- 根目录的 Python 脚本：数据下载、ETL、RQ1–RQ5 分析、绘图、基准测试与一致性检查。
- `data_hubstats/gold/analysis/`：已发布结果的 JSON/CSV 摘要，含结果清单、集中度、谱系、ML、任务生态和基准测试输出。
- `docs/`：数据质量、各研究问题、图表审查、展示素材及验收文档。
- `docs/FULL_REPRODUCTION_GUIDE.md`：完整环境配置与全量复现说明。

本公开交付包**不包含**约 1.49 GiB 的原始模型快照、约 5.8 GB 的 Spark 中间表/日志、缓存和历史备份。它们可由脚本按下述数据来源重新获取；项目输出中必要的轻量结果摘要已保留。

## 研究问题

| 编号 | 内容 |
| --- | --- |
| RQ1 | 生态如何随时间演化：月度新增模型、任务/库/标签趋势 |
| RQ2 | 下载注意力的集中程度与长尾分布 |
| RQ3 | 模型谱系网络：底座模型到衍生模型的结构 |
| RQ4 | 模型属性与下载热度的统计关联（非未来预测） |
| RQ5 | 不同任务生态的结构差异 |

## 数据与复现

- 数据源：Hugging Face 数据集 `cfahlgren1/hub-stats` 的 `models.parquet` 固定快照。
- 固定提交：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`。
- 源文件 SHA-256：`d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef`。
- 环境：Python 3.11、PySpark 4.2.0、Java 17；依赖见 `requirements.txt`。

安装依赖并获取数据后，按以下顺序运行：

```powershell
python setup_spark_fs.py
python download_hubstats.py
$env:HF_ENGINE = 'spark'; python run_full_etl.py
python verify_etl.py
python run_phase2.py
python analysis_graph_gf.py
python make_figures.py
python build_manifest.py
python write_reports.py
python check_consistency.py
```

完整的运行参数、数据契约、限制条件和 Windows 本地 Spark 配置请阅读 [完整复现指南](docs/FULL_REPRODUCTION_GUIDE.md)。

## 结果使用说明

`data_hubstats/gold/analysis/results_manifest.json` 是所有数值结论的结果清单，报告、图表和看板均以它及其引用的摘要文件为依据。研究结论应同时阅读最终报告中的数据限制：这是一个固定快照，下载指标分为近 30 天与历史累计两种口径，且标签与底座信息存在缺失。

## 目录概览

```text
.
├── data_hubstats/              # 数据指纹与轻量结果摘要（不含原始/中间数据）
├── docs/                       # 方法、结果、质量与展示文档
├── figures_final/              # Fig01–Fig12
├── jars/                       # Windows 本地 Spark 所需的额外 JVM 构件
├── *.py                        # 下载、ETL、分析、绘图及验证脚本
├── dashboard_standalone.html   # 单文件离线看板
└── report_final.md             # 最终报告
```
