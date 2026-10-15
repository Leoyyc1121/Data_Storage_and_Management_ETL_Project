# Data Storage and Management ETL Project

本项目是“数据存储与管理”课程的 ETL 综合作业，目标是构建一条可复现、可恢复、可审计的数据流水线，将 Stanford SNAP 的 Amazon 电影评论数据与电影、商品版本和人物元数据进行整合，并把最终结果加载到可查询的数据库中。

> 当前状态：任务 1 全量抽取与程序级验证完成，下载原件证据仍有待补项。任务 2 按受限替代路径完成并已推送 GitHub：253,059 个 ASIN 清单，研究元数据覆盖 73,813 个商品（29.17%），在线/授权 HTML 快照成功均为 0。任务 3 已在本地完成全量有证据分类、覆盖统计与复核：电影商品 2,955 个，待确认 246,277 个；不是合并后的作品数，未推送任务 3。任务 4—9 及 Airflow、PostgreSQL 整体环境尚未实施，下文总体架构为计划方案。实际操作、结果和上传过程统一记录在 [项目实施记录](项目实施记录.md) 中。

## 一、项目内容

项目以 Stanford SNAP 发布的 `movies.txt.gz` 为主要数据源。官方统计规模为：

- 7,911,684 条评论；
- 253,059 个不同商品；
- 889,176 个用户；
- 时间范围为 1997 年 8 月至 2012 年 10 月。

需要完成的核心工作包括：

1. 流式解析完整评论文件，并保存原始记录定位信息和异常记录；
2. 根据评论中的 ASIN 生成商品采集清单，在允许访问的范围内获取商品页面或授权快照；
3. 识别电影、非电影、合集、电视剧、演唱会等商品类型；
4. 区分电影作品与 DVD、Blu-ray、语言、区域等商品版本，并完成电影实体合并；
5. 规范化导演、主演和其他演职人员姓名，完成人物消歧；
6. 从允许使用的数据源补全上映信息，并区分首映、地区上映和商品发行时间；
7. 保存不同来源的候选值，按照可审计规则处理字段冲突；
8. 清洗评论、检测重复评论和字段异常，同时保留全部原始记录；
9. 建立字段级数据血缘，使每个最终字段都能追溯到原始证据、处理规则和运行批次；
10. 将结果加载到数据库，完成质量检查、幂等测试、恢复测试和结果查询。

## 二、总体实现方式

### 1. 技术方案

| 组件 | 计划使用的工具 | 主要职责 |
| --- | --- | --- |
| 工作流编排 | Apache Airflow | 管理任务依赖、参数、批次、重试、日志、失败分支和恢复流程 |
| 数据处理 | Python | 流式解析、清洗、候选生成、实体匹配、统计与质量检查 |
| 页面采集 | 任务 2 已使用 Python urllib + Protego；Scrapy 为可选后续工具 | 单 worker、robots 核验、显式许可检查、限速、缓存、有界重试、恢复和原始证据保存 |
| 数据库 | PostgreSQL | 保存实体、关系、候选字段、选择结果、血缘、批次和质量问题 |
| 数据访问 | SQL | 数据核对、质量检查、验收查询和演示查询 |
| 版本管理 | Git / GitHub | 管理代码、配置模板、数据库脚本和项目文档 |

Airflow 负责编排完整 ETL 流程，具体的解析、采集和匹配逻辑由 Python 或 Scrapy 实现，并由 Airflow DAG 调用。这样既能保留复杂处理的灵活性，也能提供课程要求的任务依赖、执行日志、失败恢复和可重复运行证据。

### 2. 分层架构

```text
外部数据源
   │
   ▼
Raw 原始层
   │  原始评论文件、网页响应、授权快照、第三方数据文件
   ▼
Staging 暂存层
   │  结构化评论、商品页面字段、异常记录、采集状态
   ▼
Integration 集成层
   │  电影匹配、人物消歧、信息补全、冲突处理、评论去重
   ▼
Target 目标层
      电影、商品版本、人物、评论、关系、字段选择和数据血缘
```

各层均保存 `source_id`、`run_id`、规则版本和必要的原始定位信息，避免在处理过程中丢失来源。

### 3. Airflow 工作流设计

计划建立主 DAG `amazon_movies_etl`，按以下依赖关系组织任务：

```text
下载并校验评论文件
        │
        ▼
流式解析评论 ──► 隔离异常记录 ──► 核对评论、商品和用户数量
        │
        ▼
生成 ASIN 采集清单 ──► 获取页面或快照 ──► 识别商品类型
        │
        ▼
抽取电影和人物字段 ──► 电影实体解析 ──► 人物实体解析
        │
        ▼
上映信息补全 ──► 来源冲突处理 ──► 评论清洗与重复识别
        │
        ▼
加载 PostgreSQL ──► 完整性和一致性检查 ──► 生成验收统计
```

每个任务应记录开始时间、结束时间、输入、输出、运行状态、插入/更新/隔离数量和日志位置。相同输入和规则版本重复执行时，不应产生重复实体或关系。

### 4. 数据模型概要

目标数据库计划包含以下核心实体或关系：

- `Movie`：电影作品；
- `ProductEdition`：以 ASIN 标识的商品版本；
- `ProductMovie`：商品版本与电影作品的映射，支持合集；
- `Review`、`Reviewer` 和 `ReviewMapping`：评论、用户及原始评论映射；
- `Person`、`PersonAlias` 和 `MoviePerson`：人物、别名以及导演/演员等角色关系；
- `SourceRecord`：原始文件、网页或第三方记录；
- `FieldAssertion`：来自不同来源的字段候选值；
- `MergeDecision` 和 `FieldSelection`：实体合并决定与最终字段选择；
- `ETLRun` 和 `QualityIssue`：运行批次、规则版本和质量问题。

ASIN 只作为商品版本标识，不直接作为合并后电影作品的 `movie_id`。

### 5. 可靠性与合规原则

- 完整处理数据集，不用少量样本冒充全量结果；
- 原始数据只追加或归档，不直接覆盖；
- 使用稳定 ID、唯一约束和 Upsert 保证幂等加载；
- 按批次保存检查点，支持中断后继续处理；
- 将无效记录放入隔离区，并满足“读取数 = 有效数 + 隔离数”；
- 对不可访问页面保存状态、原因和证据，不将空页面记为成功；
- 遵守 `robots.txt`、网站使用条款、请求间隔和并发限制；
- 不把密码、Cookie、Token 或其他敏感信息提交到 GitHub；
- 大型原始数据、网页快照和数据库文件不直接提交到 Git，仅登记校验值和存储位置。

## 三、仓库结构

以下为计划采用的仓库结构，目录将在对应任务开始时逐步建立：

```text
Data_Storage_and_Management_ETL_Project/
├─ README.md                       # 项目说明、实现方案与任务进度
├─ 项目实施记录.md                  # 各任务实际操作、结果、证据及 GitHub 上传记录
├─ .gitignore                      # Git 忽略规则
├─ requirements-task1.txt           # 已实现的任务 1 固定依赖版本
├─ requirements-task2.txt           # 已实现的任务 2 固定依赖，复用任务 1 依赖
├─ requirements-task3.txt           # 已实现的任务 3 固定依赖，复用任务 2 依赖
├─ requirements.txt                # Python 依赖及版本
├─ docker-compose.yml              # Airflow 与 PostgreSQL 本地运行环境
├─ .env.example                    # 环境变量模板，不包含真实密钥
├─ dags/
│  └─ amazon_movies_etl.py         # Airflow 主 DAG
├─ src/
│  ├─ extract/                     # 评论解析和商品采集
│  ├─ transform/                   # 清洗、标准化、实体解析和冲突处理
│  ├─ load/                        # 数据库加载与幂等写入
│  ├─ lineage/                     # 字段级血缘记录
│  └─ quality/                     # 数据质量与验收检查
├─ crawler/
│  ├─ spiders/                     # Scrapy 爬虫
│  └─ settings.py                  # 限速、缓存、重试和 robots.txt 设置
├─ sql/
│  ├─ schema.sql                   # 表、主外键、约束和索引
│  ├─ quality_checks.sql           # 数据质量检查
│  └─ demo_queries.sql             # 至少 5 个验收查询
├─ config/
│  ├─ task2.example.json            # 已实现的任务 2 参数和安全默认配置
│  ├─ pipeline.example.yaml        # 流水线参数模板
│  └─ rules/                       # 匹配和字段选择规则版本
├─ tests/
│  ├─ unit/                        # 单元测试
│  ├─ integration/                 # 集成测试
│  └─ fixtures/                    # 可离线运行的固定验收样本
├─ data/
│  ├─ raw/                         # 原始数据，不提交大型文件
│  ├─ staging/                     # 暂存结果
│  ├─ integration/                 # 实体匹配和候选结果
│  └─ target/                      # 导出结果或数据库备份
├─ logs/                           # 运行日志，不提交大体积日志
├─ reports/                        # 项目报告、图表和证据索引
└─ docs/                           # ER 图、数据字典和补充说明
```

## 四、任务清单

### 项目准备

- [x] 阅读并梳理项目要求
- [x] 选择 Apache Airflow 作为 ETL 编排工具
- [x] 创建中文项目报告模板
- [x] 编写项目 README
- [x] 初始化 Git 仓库并创建 GitHub 远程仓库
- [ ] 建立基础目录、依赖文件和配置模板
- [ ] 建立 Airflow + PostgreSQL 本地运行环境

### 任务 1：全量评论抽取

- [x] 下载 `movies.txt.gz` 并解压，由用户完成
- [x] 登记下载地址、下载日期及实际解析文件 `movies.txt` 的大小和 SHA-256
- [x] 实现流式、分批解析器，支持文本字段跨行及字段内空行
- [x] 生成稳定的 `source_review_id`
- [x] 保存原始记录字节位置、行号、原始哈希和异常标记
- [x] 核对全量评论数、不同 ASIN 数和非空用户 ID 数
- [x] 记录内存、磁盘占用、运行时间、解析器和规则版本
- [x] 完成 18 项自动测试、真实样本中断恢复比较及已完成全量运行的重复执行检查
- [ ] 补充原压缩文件的路径、实测大小、SHA-256 和解压一致性证据，如原件仍保留
- [ ] 补充下载方式及开始/结束时刻；无历史记录的部分如实标为未知，不伪造时间

最终运行 `full_v2` 使用解析器 `1.2.0`，得到 7,911,684 条评论、253,059 个商品、889,176 个用户，与指导文档参考值一致；输出 317 个 Parquet 分片，结构隔离数为 0。62,085 条记录的编码问题和 43 条 helpfulness 分子大于分母的问题已保留并标记，尚未视为已清洗解决。

验证范围为：全部输出文件校验值、全部记录序号和稳定 ID，以及 951 条原始字节定位抽查；真实样本恢复比较覆盖 20,000 条记录。全量重复执行采用 `--resume` 校验并复用已有结果，没有声称重新独立解析整个文件两遍。

代码见 `src/extract/`、`src/quality/` 和 `tests/unit/test_task1.py`，小型统计及验证证据见 `reports/task1/`。最终大数据结果仅在本地 `data/staging/task1/full_v2/` 保留，不上传 GitHub。在其他位置复现时，先运行来源登记程序生成当前路径的登记文件，再按 [项目实施记录](项目实施记录.md) 中的命令解析和验证。

### 任务 2：商品页面采集

- [x] 从全部 317 个任务 1 分片登记 ASIN，并保留源文件哈希、运行 ID、首条评论 ID 和逐商品评论数
- [x] 建立 253,059 行全量商品清单，包含指导文档要求的 ASIN、原始/最终 URL、状态、HTTP 状态、尝试次数、采集时间、快照位置和内容哈希
- [x] 使用指导文档允许的可比工具实现缓存、单 worker 限速、有界重试、批次恢复及逐商品恢复
- [x] 实现原始响应和授权快照保存、SHA-256 校验；本次实际保存的是访问规则预检响应，商品页面分支通过合成测试验证
- [x] 区分成功、404、受限、超时、解析失败、未采集及其他网络/HTTP 错误；规则未知与缺少许可保持未采集
- [x] 实现页面正文 ASIN 核验、重定向链和模板签名证据记录，拒绝 HTTP 200 验证码/错误页
- [x] 完成全量 ASIN/逐商品评论数核对、真实清单中断恢复和重复执行比较；优化后任务 2 共 64 项自动测试通过，连同任务 1 共 82 项
- [x] 按全部 ASIN 计算覆盖率，分别记录在线与授权快照结果，保留预检失败证据
- [ ] 取得来源允许访问的真实商品页面或指导文档允许的授权历史快照，形成可供后续任务使用的页面内容
- [x] 完成公开研究元数据全量导入：203,766 条源记录，匹配 88,660 条候选、73,813 个目标商品；逐条核对原始定位，覆盖率 29.17%，不计页面成功
- [x] 保留并标记 6 条内外 ASIN 冲突候选（4 个商品），后续不得直接自动确认为电影
- [x] 按要求文档第 3、4、10、11 节自查；核对结果见 `reports/task2/requirements_audit.json`

任务 2 的流程是 `prepare → crawl → export → verify`，由一个 `run` 命令顺序组织；任何阶段异常都会记录失败，已提交批次和证据保留。它是完整项目中任务 2 的可执行子流程，不代表 Airflow 主 DAG 或数据库加载已经完成。

本次真实结果：清单 253,059 个商品，对应 7,911,684 条评论；成功页面 0，未采集 253,059，商品请求尝试次数 0。实际声明身份下的 `robots.txt` 请求返回 HTTP 503，程序按失效关闭原则停止；同时没有可依据的在线采集许可或授权快照。不能把预检的 503 写成商品 HTTP 状态，也不能声称这些商品全部返回 404 或被永久禁止访问。清单的商品 HTTP 状态、最终 URL、采集时间和页面哈希保持 NULL，并引用共同预检证据。

默认并发为 1，请求间隔至少 3 秒，单次超时 20 秒，每个商品的在线尝试最多 3 次，重定向最多 5 次。重试遵守 `Retry-After`，不缩短服务器要求的等待时间；收到 401/403/429 或验证码页时停止该来源，持久保存停止状态。机器人身份不伪装成浏览器，也不通过改身份绕过限制。robots 允许访问不等同于条款允许；在线商品来源需有实际使用依据。优化版已移除额外的 `live_permission` 布尔开关，不再要求同时满足一个预设关闭的重复开关。

在项目根目录的 PowerShell 执行：

```powershell
& .\.venv\Scripts\python.exe -m pip install -r requirements-task2.txt
& .\.venv\Scripts\python.exe src\extract\acquire_products.py run --config config\task2.example.json
# 可单独重复核验已有结果，不发网络请求。
& .\.venv\Scripts\python.exe src\quality\verify_task2.py --config config\task2.example.json
& .\.venv\Scripts\python.exe -m unittest discover -s tests/unit -p test_task2.py -v
```

直接重跑会核对输入和已提交批次，复用已完成状态；失败项只有显式使用 `--retry-failed` 才进入重处理选择，该参数不绕过 robots、来源条款或持久停止状态。成功预检缓存 1 小时；失败预检仅缓存 30 秒，临时 5xx/网络异常最多尝试 3 次并保存每次响应。可以加 `--refresh-policy` 强制重新核验，但始终使用同一身份，不能忽略禁止规则。规则文件 404/410 与服务器 5xx 分开处理，前者表示没有取得已发布规则，不等于获得条款许可。

### 任务 2 优化版与公开研究输入

优化版为 `1.1.0`，HTTPS 使用 `truststore==0.10.5` 的系统信任库，保持证书校验开启，解决本机 Python 无法构建 UCSD 证书链的问题。新的研究输入入口是 `src/extract/acquire_product_metadata.py`，配置为 `config/task2.metadata.example.json`；提供者为 [UCSD Amazon Review Data 2018](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/)，只取得官网明确链接的 Movies and TV 元数据 gzip，不抓取 Amazon 受限网页。

研究来源单独保存压缩原件、下载时间、SHA-256、官方来源依据及文件服务器 robots 证据。逐条保留原始 JSON 字节、原始哈希、源档案哈希、记录序号和解压字节起止位置，用 ASIN 精确筛选目标商品，保留重复候选和异常，不提前判断电影实体或选择最终字段。历史页面采集时间未知，不用本次文件下载时间冒充历史时间。

```powershell
# 明确重新核验原来源，临时故障只作有限重试。
& .\.venv\Scripts\python.exe src\extract\acquire_products.py crawl --config config\task2.example.json --refresh-policy --max-products 10

# 可将公开研究来源接入同一任务 2 子流程；结果不计为页面成功。
& .\.venv\Scripts\python.exe src\extract\acquire_products.py run --config config\task2.example.json --metadata-config config\task2.metadata.example.json

# 或单独恢复已开始的研究档案获取与导入。
& .\.venv\Scripts\python.exe src\extract\acquire_product_metadata.py --config config\task2.example.json --source-config config\task2.metadata.example.json
```

研究档案不等于 HTML 页面或教师指定快照，指标在 `metadata_statistics.json` 单独报告；不能用它抬高 `success` 或在线采集成功率。研究来源要求引用 Ni、Li、McAuley 的 EMNLP 2019 论文，引用与用途限定已登记。原件完整读至 EOF 并通过 gzip CRC 和原始定位核验；官网标注 203,970 个商品，而本文件实际有 203,766 行，统计口径/文件版本差异尚无确证，不凑数，也不据此声称缺失 204 个目标商品。179,246 个目标商品没有研究元数据，后续须保持待确认。Git 仅提交实现、文档和小型统计/验证证据，不公开原始研究档案或逐商品内容。

清单和检查点位于 `data/staging/task2/full/`，原始预检响应位于 `data/raw/task2/policy/`。`reports/task2/` 保存全量统计、预检记录、核验、测试、中断恢复、重复执行及执行日志。详细字段、快照导入契约、命令和限制见实施记录的任务 2 章节。

### 任务 3：电影商品识别

- [x] 定义电影、非电影、合集、电视剧、演唱会、其他内容和待确认七种互斥状态；DVD/Blu-ray 标题不能单独证明是电影
- [x] 保存逐候选的分类证据、源档案/记录哈希、原始字节定位、规则版本和运行 ID；缺少资料或候选冲突保持待确认
- [x] 保留全部商品与原始评论引用，不生成 `movie_id`、不删除评论、不复制合集评论
- [x] 完成真实全量分类、逐条原始证据复核与字段覆盖统计，验证中断恢复和重复执行；任务 3 的 30 项测试通过，全部任务共 112 项通过

任务 3 入口 `src/transform/classify_products.py` 组织 `核验任务2输入 → 按ASIN批处理分类 → 导出 → 全量复核`；规则为 `config/task3.rules.json`，运行参数为 `config/task3.example.json`。当前适配实际可用的公开研究 JSON，而不是声称处理了不存在的 HTML。将来取得成功网页时，当前适配器会明确拒绝静默忽略，须增加并核验 HTML 字段适配器后重算。

```powershell
& .\.venv\Scripts\python.exe -m pip install -r requirements-task3.txt
& .\.venv\Scripts\python.exe src\transform\classify_products.py --config config\task3.example.json
& .\.venv\Scripts\python.exe src\quality\verify_task3.py --config config\task3.example.json
& .\.venv\Scripts\python.exe -m unittest discover -s tests/unit
```

最终规则版本 `task3-conservative-content-v2`，引擎 `1.1.0`。分类输出 `data/staging/task3/full_v2/product_classification.parquet` 每个 ASIN 一行；证据输出 `classification_evidence.parquet` 保留全部 88,660 条候选，而不是最后一条覆盖前面的记录。`classification.sqlite3` 是该步骤的恢复检查点，不是项目最终目标库。候选字段保存原始值、规范候选、字段定位和缺失原因，商品类型不确定也不丢弃字段证据。

| 分类 | 商品数 |
| --- | ---: |
| 电影 | 2,955 |
| 非电影（教学、健身等） | 2,232 |
| 合集 | 137 |
| 电视剧 | 1,143 |
| 演唱会 | 308 |
| 其他内容 | 7 |
| 待确认 | 246,277 |
| 合计 | 253,059 |

电影商品识别覆盖率 1.17%，待确认率 97.32%；低识别覆盖来自来源缺失及保守规则，不强行填满。待确认包括无研究资料的 179,246 个、证据不足/歧义 67,027 个及身份冲突 4 个。全部 7,911,684 条评论引用保留。电影商品候选字段覆盖率：标题 100%、类型 42.20%、语言 54.75%、字幕 28.87%；导演、主演/演员及上映信息未获得可靠候选，缺失不伪造。完整分母和其他字段见 `reports/task3/full_statistics.json`。

字段覆盖率分别报告“全部商品”和“已识别电影商品”的分母，指当前候选是否可得，并非最终选定或验证的作品字段。作品级覆盖率保持 NULL，原因是任务 4 尚未建立电影作品；导演/主演/演员和上映信息没有可靠来源时也保持 NULL，不把品牌当导演，不从标题年份或最早评论推造上映日期。在线、授权 HTML 快照、研究来源三项继续分别统计，页面成功率分母始终是全部 253,059 个商品。

重复执行会核验输入和规则指纹、复用已提交决定；`--stop-after-batches 1` 可主动中断（退出码 130），再执行原命令继续。更换规则文件、源数据或实现哈希时须指定新的 `work_dir` / `report_dir`，保留旧决定版本，不覆盖历史。任务 3 本轮仅本地实现，不包含在任务 2 的 GitHub 推送中。

复查修正了首轮把描述中历史/附赠的音乐视频提及当成主要内容的问题，并增加回归测试；最终规则只接受主内容证据。首轮 `data/staging/task3/full/` 和 `reports/task3/rule_v1_diagnostic.json` 是保留的旧决定，不是最终结果；新旧目录各有准确规则/实现/配置快照，哈希与各自输入登记一致。全量验证、恢复、重复执行和要求自查分别见 `full_verification.json`、`recovery.json`、`idempotency.json`、`requirements_audit.json`。样例检查和规则重算不是独立人工标注评估，不报告分类准确率。

### 任务 4：电影实体合并

- [ ] 定义“同一电影作品”的操作规则
- [ ] 规范化标题、语言、年份和商品描述
- [ ] 生成候选匹配并进行多字段验证
- [ ] 区分自动合并、人工复核、拒绝和未解决状态
- [ ] 保存置信度、证据、阈值和规则版本
- [ ] 检查传递性冲突
- [ ] 准备跨版本合并和相似但不合并的案例

### 任务 5：人物姓名归一与消歧

- [ ] 生成稳定的 `person_id`
- [ ] 保存规范姓名、原始姓名、别名和外部 ID
- [ ] 基于作品、角色、年代等证据消歧
- [ ] 将导演、主演和其他演员建模为关系
- [ ] 准备人物别名合并及同名不同人的案例

### 任务 6：上映信息补全

- [ ] 定义首映、地区上映和商品发行时间的语义
- [ ] 完成电影身份匹配后再补全信息
- [ ] 分别保存日期、年份、精度、地区和上映类型
- [ ] 保存来源、原始值、提取时间、证据和置信度
- [ ] 将推断上界与已验证日期分开保存

### 任务 7：来源冲突处理

- [ ] 为标量和多值字段定义选择规则
- [ ] 保存全部候选值及采用、拒绝或未解决状态
- [ ] 记录最终选择理由和规则版本
- [ ] 保留合并后的原始评论、版本属性和来源记录

### 任务 8：评论清洗与重复识别

- [ ] 定义重复候选键和核验规则
- [ ] 保留全部原始评论并建立逻辑评论映射
- [ ] 检查 helpfulness 分子、分母和负数异常
- [ ] 检查评分范围和时间戳异常
- [ ] 明确电影评分统计的分母及合集处理方式

### 任务 9：字段级数据血缘

- [ ] 保存来源记录及稳定定位信息
- [ ] 保存字段候选值、规范值和提取规则版本
- [ ] 保存父子处理关系、操作类型、`rule_id` 和 `run_id`
- [ ] 保存最终字段选择及选择理由
- [ ] 支持从最终字段追溯到原始证据
- [ ] 实现课程要求的血缘查询

### 数据库、质量与验收

- [ ] 完成 ER 图、数据字典和数据库脚本
- [ ] 实现主键、外键、唯一约束和索引
- [ ] 完成全量数量与状态互斥性核对
- [ ] 建立电影和人物各不少于 100 对的独立评估集
- [ ] 计算 Precision、Recall 和 F1
- [ ] 完成幂等加载测试
- [ ] 完成中断与恢复测试
- [ ] 准备离线端到端验收样本
- [ ] 编写至少 5 个可执行演示查询
- [ ] 完成项目报告和现场演示材料

## 五、主要交付物

交付范围仅依据本地指导文档 `Data_Storage_and_Management_ETL_Assignment_English.docx` 第 11 节，并按第 8—10 节细化。文档没有要求把所有工作目录或试验历史上传，也没有指定报告必须采用 Word 格式。当前仓库只保留已形成的交付材料，不代表整个项目已经完成。

| 指导文档要求 | 当前已形成的材料 | 仍待完成的主要材料 |
| --- | --- | --- |
| 项目报告 | `项目实施记录.md`：任务 1—3 的事实、方法、统计、限制及后续报告素材；任务 3 本地未推送 | 覆盖全部任务的正式项目报告；当前 Word 草稿不作为正式交付 |
| 完整实现 | 抽取/采集、任务 3 分类、质量核验、任务 1—3 自动测试和固定依赖、任务 2/3 参数与分类规则 | 作品/人物匹配、评论清洗、血缘、整体 ETL 作业定义与失败分支、数据库模型及加载脚本、整体配置模板；ER 图和数据字典 |
| 数据与证据 | 任务 1 最终登记和全量证据；任务 2 全量商品清单、预检失败证据、公开研究原件、统计和校验（大型内容仅登记本地位置/哈希） | 后续异常与合并决定、字段血缘、独立评估集及固定离线端到端验收样本；页面原目标受访问条件限制，未宣称完成全量 HTML 获取 |
| 演示与复现说明 | 本 README、实施记录中的环境/资源和命令、任务 1、2 日志及恢复/重复执行证据、人员及 AI 辅助记录 | 完整端到端运行说明、目标库幂等加载与恢复证据、规则变更重算演示和至少 5 个可执行结果查询 |

任务 1 的 Git 证据清单如下，其他旧版统计和试验日志仅保留在本地：

- `source_manifest.reported.json`：最终采用的输入文件大小、哈希和已有下载信息，未知项保持未知。
- `full_v2_statistics.json`、`full_v2_verification.json`、`full_v2_idempotency.json`、`full_v2.log`：最终全量统计、检查和执行证据。
- `full_v2_output_manifest.json`：317 个 Parquet 分片的相对路径、行数、大小及 SHA-256，以及实际本地存储位置。
- `sample_v2_20k_statistics.json`、`sample_recovery_v2_20k_statistics.json`、`sample_recovery_v2_comparison.json` 及两份对应的 `*.log`：独立样本与中断恢复比较证据。
- `multiline_diagnosis.json`：解释首轮多计 12 条的原因，支撑指导文档要求的数量差异说明，不作为最终统计。

上述证据位于 `reports/task1/`。`.gitignore` 负责排除大型数据、运行环境、凭据、旧版中间结果及未完成的报告草稿；`.gitkeep` 仅用于保留输入目录。最终报告完成后应解除对应草稿忽略规则并提交。

### 大型数据的存储与恢复

按第 11 节要求，大型数据不直接进入 Git，而提交校验值、存储位置和恢复说明：

- 原始输入：本机项目目录下 `data/raw/movies.txt`；9,332,887,470 字节，SHA-256 见最终来源登记及实施记录。
- 结构化输出：本机 `D:\My projects\Data_Storage_and_Management_ETL_Project\data\staging\task1\full_v2`，共 317 个分片、7,911,684 条记录；逐文件校验值见 `full_v2_output_manifest.json`。这些文件未托管在 GitHub，克隆仓库不会得到数据本体。
- 恢复输入：从来源登记中的 Stanford SNAP 地址取得 `movies.txt.gz`，解压后计算 `movies.txt` 的 SHA-256，核对与登记版本一致；当前尚缺压缩原件实测哈希，不能声称已验证压缩原件。
- 重建结果：安装 `requirements-task1.txt` 的固定依赖，使用来源登记程序登记当前机器上的输入路径，再按照实施记录的命令，以解析器 `1.2.0`、规则 `task1-preserve-raw-v3`、批大小 `25000` 和编码策略 `unresolved` 运行解析及独立验证。不要直接沿用历史登记中的另一台机器绝对路径。
- 中断恢复：已有同一运行的分片及 `checkpoint.sqlite3` 时使用 `--resume`；检查点丢失时，在新的运行目录从输入重新解析，不能仅凭 JSON 统计继续旧批次。
- 任务 2 全量清单：本机 `data/staging/task2/full/product_manifest.parquet`，253,059 行、12,205,020 字节，SHA-256 见 `reports/task2/full_statistics.json`。按任务 2 命令从任务 1 输出重建；历史网络预检快照应另行保留，重新访问不保证得到相同响应。清单和原始响应不直接上传 GitHub，提交统计、校验值和恢复说明。
- 任务 2 研究输入：本机 `data/raw/task2/third_party/ucsd_amazon_2018_movies_tv/meta_Movies_and_TV.json.gz`，105,021,691 字节；匹配输出 `data/staging/task2/full/third_party_product_records.parquet`，88,660 行、62,907,673 字节。两者 SHA-256、来源 URL、来源依据、时间和本地位置见 `reports/task2/metadata_statistics.json`。先恢复任务 1 与任务 2 全量清单，再运行 `acquire_product_metadata.py` 重取/导入；必须核对原件哈希，来源文件变化时建立新版本运行，不沿用旧检查点。仅重新导出时可运行 `acquire_products.py export --metadata-config config/task2.metadata.example.json`，缓存原件会经过校验，不重新访问网站。
- 任务 3 最终结果：本机 `data/staging/task3/full_v2/`，分类文件 253,059 行、14,240,606 字节，证据文件 88,660 行、16,228,406 字节。文件及稳定字段哈希见 `reports/task3/full_statistics.json`；先恢复任务 2 的同版本输入，再按任务 3 命令重建。规则/实现/配置精确快照同存于版本目录；丢失检查点须新目录重算，不凭统计续跑，旧规则决定不覆盖。新运行 ID 可导致二进制哈希不同，应同时核对输入/规则哈希和语义字段。

分片 SHA-256 标识本次保存的二进制文件。重新生成的 `run_id` 或 Parquet 运行库不同，文件哈希可能变化；应同时核对输入哈希、规则、记录数和稳定 ID，不能把新运行的二进制差异直接认定为数据错误。任务 1 的抽取测试不替代完整项目的目标库验收。

## 六、数据来源

- [Stanford SNAP Amazon Movie Reviews](https://snap.stanford.edu/data/web-Movies.html)
- [Scrapy Documentation](https://docs.scrapy.org/en/latest/)
- [Protego robots.txt parser](https://github.com/scrapy/protego)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [IMDb Non-Commercial Datasets](https://datasets.imdbws.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 七、说明

本仓库仅提交允许公开的代码、配置模板、数据库脚本、文档和小型验收样本。原始大型数据、受使用条款限制的网页内容、真实凭据和敏感信息不会直接提交到公开仓库。
