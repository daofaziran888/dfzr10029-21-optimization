# 数据分析实验与数据应用服务平台

面向指标口径管理、分析实验设计、OLAP查询、数据看板构建与数据API发布的数据应用平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 指标定义 | `metric_def` | 指标 | 指标编号、指标名称、计算公式 |
| 维度管理 | `dimension` | 分析维度 | 维度编号、维度名称、维度类型 |
| 数据集管理 | `dataset` | 分析数据集 | 数据集编号、数据集名称、来源类型 |
| 分析实验 | `experiment` | 实验方案 | 方案编号、实验名称、分析数据集 |
| OLAP查询 | `query_builder` | 查询模板 | 模板编号、模板名称、查询语句 |
| 数据看板 | `dashboard` | 看板配置 | 看板编号、看板名称、包含图表 |
| 图表配置 | `chart` | 图表 | 图表编号、图表名称、图表类型 |
| 数据API发布 | `data_service` | 数据服务 | 服务编号、服务名称、输出数据集 |
| 智能告警 | `alert` | 告警规则 | 规则编号、告警名称、监控指标 |
| 分析用户组 | `user_group` | 用户组 | 组编号、组名称、组描述 |
| 数据快照 | `snapshot` | 快照记录 | 快照编号、数据集名称、快照时间 |
| 任务调度 | `scheduler` | 调度任务 | 任务编号、任务名称、调度类型 |
| 缓存管理 | `cache` | 缓存配置 | 缓存编号、缓存目标、缓存策略 |
| 分析管道 | `pipeline` | 分析节点 | 节点编号、输入数据集、分析算子 |
| 分析模型 | `model` | 模型配置 | 模型编号、模型名称、模型类型 |
| 数据集市 | `data_mart` | 集市表 | 表编号、表名称、主题域 |
| 数据导出 | `export` | 导出任务 | 任务编号、导出对象、导出格式 |
| 数据审计 | `audit` | 审计记录 | 记录编号、操作类型、操作人 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 任务调度：补跑窗口合并与状态推进

针对离线恢复时同一任务连续触发多次补跑、后一次覆盖前一次、队列积压已完成周期的问题，
调度模块在既有时间线之外增加了补跑窗口核对/合并链路（`app/services/catchup.py`，
接口前缀 `/api/scheduler/catchup`）：

1. **来源记录原样保留**：调度引擎、离线恢复、人工补录上报的错过窗口写入
   `scheduler_missed_sources`，同一任务同一周期的重复上报不去重，作为核对输入。
2. **核对与合并在同一推进边界内完成**：`POST /reconcile` 读取当前待核对来源，按
   「来源优先级 → 最晚记录时间」裁决冲突，生成唯一核对条目并把相邻窗口合并为补跑批次；
   一次提交只保留一份有效版本（`scheduler_catchup_versions.is_current`），旧版本转历史。
3. **同一周期不重复执行**：已建批次（含已归档）和未归档执行记录覆盖的周期会被占用保护，
   重复来源不会再生成批次；未归档执行记录中「任务+周期」唯一，重试复用原记录不复制。
4. **状态推进与归档留痕**：批次 待补跑→补跑中→已完成/执行失败→已归档 的每次切换都写
   `scheduler_catchup_events`，并同步执行记录（最终产物 `scheduler_executions`）与任务状态；
   完成的周期立即离开积压队列（`GET /queue`）。
5. **两种合并范围**：`merge_scope=task` 按任务分别合并，`group` 按依赖组整体合并
   （组内窗口并集扩散到同组任务），两种方式产出不同的批次与队列长度，由同一唯一性约束兜底。
6. **页面与导出**：调度页展示合并后的补跑批次、最终执行状态、来源记录与生命周期流水；
   `GET /export/batches` 逐行列出被合并周期，`GET /export/executions` 导出执行记录。
   `GET /integrity` 返回周期唯一性、有效版本唯一性等核对结果。

后端核对脚本：`backend/tests/test_catchup_flow.py`；
前端端到端：`frontend/tests/scheduler-catchup.spec.ts`。

领域：数据工程
子方向：数据分析与数据应用
