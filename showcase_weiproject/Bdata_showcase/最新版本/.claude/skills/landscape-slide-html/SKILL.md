---
name: landscape-slide-html
description: 把答辩/汇报/课程项目的素材（文字稿、数据结论、配图）封装成横板（横向阅读）演示网页。面向计算机专业学生的成果展示，采用 Claude 网页风格的配色与字体，专业且具设计感，包含动画与交互，自适应屏幕。当用户要求"生成横板阅读的 HTML 网页 / PPT 式网页 / 答辩展示网页 / 把素材做成网页"时使用。
---

# 横板演示网页制作 Skill

## 一、技能定位（何时使用）

- 用户需要把「文字稿 + 数据 + 图片」做成一页页**横板（横向翻页）阅读**的网页，用于答辩、结题汇报、项目展示。
- 目标受众是**计算机专业大二学生 / 评审老师**，要求**专业 + 有设计感 + 信息详尽**。
- 视觉参照 **Claude 网页**：米白暖色底、陶土色强调、衬线标题、无衬线正文。
- 必须有**动画与交互**、**首页与尾页突出主题**、**内容精确**、**多种布局**、**自适应屏幕不遮挡内容**。

## 二、工作流程（按顺序执行）

1. **扫描素材目录**：列出所有文件，区分文本稿（.txt / .md）与图片（.png / .jpg）。
2. **提取内容**：读取文字稿，抽取「题目 / 成员 / 研究问题(RQ) / 数据 / 方法 / 问题与解决 / 结论与发现 / 动机 / 未来」等结构化信息；从配图说明稿中建立「图片文件名 → 归属页 → 图注」的映射。
3. **核实图片**：确认每个 `src` 引用的文件名与磁盘上的实际文件名**完全一致**（含中文、大小写）。
4. **确定页面结构**：通常 15–18 页（见下文模板），可拆分密集页、加章节过渡页，但**不更改主要内容**。
5. **搭建骨架**：单文件 HTML，含 `<style>` 与 `<script>`，图片用相对路径。
6. **应用设计系统**：先写 CSS 变量与组件类，再写页面。
7. **写内容与布局**：每页用不同的布局模式（左右交错、多栏、统计带、数据条等）。
8. **添加交互动画**：翻页、数字滚动、数据条、灯箱、选项卡、粒子背景、入场 reveal。
9. **质量校验**：跑校验脚本（见第九节），确认 0 错误后才交付。
10. **交付说明**：告知文件路径、如何打开、需替换的占位符。

## 三、设计系统（Claude 风格）

### 配色（CSS 变量）
```css
:root{
  --bg:#faf9f5;      /* 米白底 */
  --bg2:#f3efe6;     /* 次底色 */
  --card:#ffffff;    /* 卡片 */
  --card2:#f7f4ee;   /* 浅色卡片 */
  --ink:#1f1e1d;     /* 主文字 */
  --ink2:#5b564d;    /* 次级文字 */
  --ink3:#8f897d;    /* 弱化文字 */
  --line:#e7e2d7;    /* 描边 */
  --line2:#efece4;   /* 浅描边 */
  --accent:#d97757;  /* 陶土色强调（Claude 主色） */
  --accentDark:#b9553a;
  --accentSoft:#f5e5dd;
  --green:#4f7d5c; --blue:#5b7da8; --gold:#c09a4b; /* 数据条配色 */
}
```

### 字体栈
```css
--serif:"Noto Serif SC","Source Han Serif SC","STZhongsong","STSong","SimSun",Georgia,serif; /* 标题 */
--sans:"Noto Sans SC","Source Han Sans SC","PingFang SC","Microsoft YaHei",system-ui,sans-serif; /* 正文 */
--mono:"IBM Plex Mono","SF Mono","JetBrains Mono",Consolas,monospace; /* 数字/标签 */
```
> 通过 Google Fonts 引入 Noto Serif/Sans SC，离线时自动回退系统字体。

### 排版规范
- 字号一律用 `clamp(最小, 视口比例, 最大)`，随屏幕缩放：标题 `clamp(30px,3.6vw,56px)`、正文 `clamp(15px,1.15vw,18px)`、大数字 `clamp(30px,3.2vw,52px)`。
- 卡片圆角 16–18px，细描边 + 极浅阴影；左侧小色块/圆点做分区标签。

## 四、页面结构模板（18 页示例）

1. 封面（主题大字 + 粒子网络背景 + 成员信息占位）
2. 目录（可点击跳转的编号列表）
3. 前置知识
4. 选题 / 研究问题概览（多卡片并列）
5. 测试环境
6. 数据集特征
7. 【章节过渡页】研究问题
8–12. RQ1–RQ5（每个 RQ 按五段式编排）
13. 【章节过渡页】稳健性·总结·展望
14. 稳健性与总体结论
15. 工作动机与创新点
16. 未来打算
17. 尾页（Q&A + 结论一句话 + 粒子背景）

> RQ 页统一五段式：**研究问题 / 数据 / 方法 / 问题与解决 / 结论与发现**，配主图 + 备用小图 + 底部统计带。

## 五、布局模式库（多种布局交替使用）

| 类名 | 作用 |
|---|---|
| `.cols` | 两等栏 |
| `.cols-21` / `.cols-12` | 2:1 或 1:2 不等栏（左右交错用） |
| `.cols-3` / `.cols-4` | 三/四列卡片 |
| 内联 `grid-template-columns:repeat(5,1fr)` | 五卡并列（选题页） |
| `.stats` / `.stats.c2` / `.stats.c3` | 底部统计带（大数字） |
| `.bars` + `.bar-row` | 动画数据条（见第七节） |
| `.pipe` | 训练流水线色块条 |
| `.agenda` | 目录列表 |
| `.divider` | 章节过渡页（超大轮廓数字） |
| `.tabs` + `.tabpane` | 选项卡（三案例切换） |

## 六、交互与动画清单

1. **翻页**：`#track{transition:transform .72s cubic-bezier(.77,0,.18,1)}`，JS 用 `translateX(-index*100vw)`。
2. **导航**：方向键 / 空格 / 滚轮（带防抖）/ 上一页下一页按钮 / 底部圆点（可点击、当前页拉长高亮）/ 顶部进度条 + 页码。
3. **入场 reveal**：`.reveal{opacity:0;transform:translateY(26px)}`，`.slide.on .reveal` 显现，用 `.d1`–`.d7` 错峰。
4. **数字滚动 count-up**：`[data-count]` 元素进入页面时从 0 动画到目标值（整数用 `toLocaleString`，小数用 `toFixed`）。
5. **动画数据条**：`.bar-fill{width:0}`，`.slide.on .bar-fill{width:var(--w)}`，`--w` 内联写真实百分比，条长严格正比于数据。
6. **灯箱 lightbox**：点击任意图放大查看 + 图注，Esc/点击关闭。
7. **选项卡 tabs**：切换不同案例图。
8. **粒子网络 canvas**：封面/尾页背景，节点随机漂移 + 距离连线，`LINK = min(w,h)/6`。
9. **图片悬停**：上浮 + 放大镜提示。

## 七、自适应方案（关键：保证不遮挡、不留黑边）

1. 舞台用**流体全屏**，不用固定尺寸缩放：
```css
#deck{position:fixed;inset:0;width:100vw;height:100vh;overflow:hidden}
.slide{flex:0 0 100vw;width:100vw;height:100vh;overflow-x:hidden;overflow-y:auto}
```
2. 字号/间距全部用 `clamp()` + `vw/vh`，随屏幕等比缩放。
3. 图片用「最大高度 + 等比缩放」，避免竖长图把版面撑爆：
```css
.fig img{width:100%;object-fit:contain;max-height:clamp(130px,26vh,260px)}
.fig.big img{max-height:clamp(190px,40vh,430px)}
```
4. **兜底**：`.slide{overflow-y:auto}` —— 万一一屏装不下，页面内滚动而**绝不裁切内容**。

## 八、数据可视化规范（内容精确）

- 所有数字只来自同一份结果清单，**不写图里没有的数字**。
- 数据条 `--w` = 真实数值 × 100%（如 0.413 → 41.3%、38.5% → 38.5%），右侧标注精确值。
- 图注必须带上**限制条件**（如「未抽样、未插值」「50 个可比任务需带阈值」「直系规模只是 1 跳」）。
- 缺失值（任务 65.55%、血缘 71.74%）显式报告，不推断、不填补。

## 九、质量校验清单（交付前必跑）

用 Python（内置 `html.parser`）校验，确认：
- `sections` 数量 = 预期页数；
- 所有 `<img src>` 引用的文件**真实存在**（0 缺失）；
- HTML 标签**全部配平**（0 unclosed / 0 mismatch）；
- 无残留占位符（如 `__CSS__`、`__SLIDES__`）；
- 无废弃变量/函数引用（如删除缩放后残留 `SW`、`fit()`）。

校验脚本核心（保存为临时 `.py` 运行）：
```python
import re, os
from html.parser import HTMLParser
d = r'<素材目录>'
s = open(os.path.join(d,'index.html'),encoding='utf-8').read()
VOID={'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'}
class P(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.stack=[]; self.errs=[]
    def handle_starttag(self,t,a):
        if t not in VOID: self.stack.append(t)
    def handle_endtag(self,t):
        if t in VOID: return
        if self.stack and self.stack[-1]==t: self.stack.pop()
        else: self.errs.append(t)
p=P(); p.feed(s); p.close()
print('unclosed:', p.stack, 'errors:', p.errs)
srcs = re.findall(r'src="([^"]+)"', s)
print('missing imgs:', [x for x in srcs if not os.path.exists(os.path.join(d,x))])
```

## 十、常见坑与规避

- **内容被右侧/底部遮挡**：不要用「固定 16:9 画布 + scale 缩放」（缩放基准算错会溢出）；改用流体 100vw×100vh + clamp + `overflow-y:auto` 兜底。
- **字体在框里显小**：字号用 vw 比例的 clamp，且收紧卡片 padding。
- **竖长图撑爆版面**：给 `.fig img` 加 `max-height` + `object-fit:contain`。
- **中文文件名编码**：校验图片存在性用 Python（UTF-8），不要用 PowerShell 5.1 直接读 UTF-8 无 BOM 文件（会乱码误判）。
- **主图与小图分级**：主图加 `.big` 类（max-height 更大），其余做缩略图点击放大。
- **占位符**：封面/尾页的班级、成员、指导教师、日期用 `＿＿＿＿` 占位，交付时提醒替换。
