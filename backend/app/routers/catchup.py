"""补跑窗口核对、合并与调度状态推进接口。

路由挂在 /api/scheduler/catchup 下，与既有时间线接口（/api/scheduler）共存且互不影响。
列表类接口返回 {items, ...}，动作类接口返回 {ok, message, ...}，与平台约定保持一致。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.catchup import MERGE_SCOPES, catchup_service

router = APIRouter(prefix="/api/scheduler/catchup", tags=["任务调度-补跑窗口"])


def csv_cell(value: Any) -> str:
    """CSV 单元格：只做公式注入前缀转义，引号交给 csv.writer 统一处理。"""
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        return "'" + text
    return text


class ReconcilePayload(BaseModel):
    merge_scope: str = Field(default="task", description="task=按任务分别合并；group=按依赖组整体合并")
    remark: str | None = None


class SourcePayload(BaseModel):
    """跨系统模块（调度引擎/离线恢复/人工补录）上报的错过窗口来源记录。"""

    task_code: str
    period: str
    source: str = "offline-recovery"
    priority: int = 2
    recorded_at: str | None = None
    remark: str | None = None


class BatchActionPayload(BaseModel):
    remark: str | None = None


# ------------------------------------------------------------------ 来源记录
@router.post("/sources")
def ingest_source(payload: SourcePayload) -> dict[str, Any]:
    """跨系统模块上报错过窗口来源记录；同一周期多次上报原样保留，由核对环节裁决去重。"""
    row = catchup_service.ingest_source(
        task_code=payload.task_code,
        period=payload.period,
        source=payload.source,
        priority=payload.priority,
        recorded_at=payload.recorded_at,
    )
    return {"ok": True, "message": "来源记录已登记，等待核对", "source": row}


@router.get("/sources")
def list_sources(
    task_code: str | None = Query(default=None),
    source: str | None = Query(default=None),
) -> dict[str, Any]:
    """跨系统模块读取补跑窗口清单来源记录（核对输入，原样保留重复上报）。"""
    items = catchup_service.list_sources(task_code=task_code, source=source)
    return {"items": items, "total": len(items)}


# ------------------------------------------------------------------ 核对合并
@router.post("/reconcile")
def reconcile(payload: ReconcilePayload) -> dict[str, Any]:
    """读取当前来源记录，合并错过窗口并推进调度状态；一次提交只生成一份有效版本。"""
    summary, message = catchup_service.reconcile(merge_scope=payload.merge_scope, remark=payload.remark)
    if summary is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, **summary}


@router.get("/versions")
def list_versions() -> dict[str, Any]:
    """补跑窗口清单核对结果版本列表，当前有效版本以 is_current 标记。"""
    items = catchup_service.list_versions()
    return {"items": items, "total": len(items), "current": catchup_service.current_version()}


@router.get("/items")
def list_items(
    version: str | None = Query(default=None),
    task_code: str | None = Query(default=None),
) -> dict[str, Any]:
    """回查补跑窗口清单核对明细；默认返回当前有效版本。"""
    items = catchup_service.list_items(version=version, task_code=task_code)
    return {"items": items, "total": len(items)}


# ------------------------------------------------------------------ 批次队列
@router.get("/batches")
def list_batches(
    status: str | None = Query(default=None),
    task_code: str | None = Query(default=None),
) -> dict[str, Any]:
    items = catchup_service.list_batches(status=status, task_code=task_code)
    return {"items": items, "total": len(items), "queue": catchup_service.queue_summary()}


@router.get("/queue")
def queue_summary() -> dict[str, Any]:
    return catchup_service.queue_summary()


@router.post("/batches/{batch_no}/start")
def start_batch(batch_no: str, payload: BatchActionPayload) -> dict[str, Any]:
    entry, message = catchup_service.start_batch(batch_no)
    if entry is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "batch": entry}


@router.post("/batches/{batch_no}/complete")
def complete_batch(batch_no: str, payload: BatchActionPayload) -> dict[str, Any]:
    entry, message = catchup_service.complete_batch(batch_no)
    if entry is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "batch": entry}


@router.post("/batches/{batch_no}/fail")
def fail_batch(batch_no: str, payload: BatchActionPayload) -> dict[str, Any]:
    entry, message = catchup_service.fail_batch(batch_no, reason=payload.remark)
    if entry is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "batch": entry}


@router.post("/batches/{batch_no}/archive")
def archive_batch(batch_no: str, payload: BatchActionPayload) -> dict[str, Any]:
    entry, message = catchup_service.archive_batch(batch_no)
    if entry is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "batch": entry}


# ------------------------------------------------------------------ 执行记录
@router.get("/executions")
def list_executions(
    task_code: str | None = Query(default=None),
    include_archived: bool = Query(default=True),
) -> dict[str, Any]:
    """数据合并后的调度执行记录（最终产物）。"""
    items = catchup_service.list_executions(task_code=task_code, include_archived=include_archived)
    return {"items": items, "total": len(items)}


@router.get("/events")
def list_events(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    """生命周期切换与归档动作流水。"""
    items = catchup_service.list_events(batch_no=batch_no)
    return {"items": items, "total": len(items)}


@router.get("/integrity")
def integrity() -> dict[str, Any]:
    """一致性核对：同一周期不会连续重复执行、有效版本唯一等。"""
    return catchup_service.check_integrity()


# ------------------------------------------------------------------ 导出
@router.get("/export/batches")
def export_batches() -> StreamingResponse:
    """导出补跑批次，逐行列出被合并的周期。"""
    items = catchup_service.list_batches()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    header = ["批次编号", "任务编号", "依赖组", "合并范围", "窗口周期清单", "被合并周期数", "状态", "合并来源版本", "创建时间", "更新时间"]
    writer.writerow(header)
    for item in items:
        periods = item.get("窗口周期清单", [])
        writer.writerow([
            csv_cell(item.get("批次编号")),
            csv_cell(item.get("任务编号")),
            csv_cell(item.get("依赖组")),
            csv_cell(item.get("合并范围")),
            csv_cell(";".join(periods)),
            len(periods),
            csv_cell(item.get("状态")),
            csv_cell(item.get("合并来源版本")),
            csv_cell(item.get("创建时间")),
            csv_cell(item.get("更新时间")),
        ])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=scheduler_catchup_batches.csv"},
    )


@router.get("/export/executions")
def export_executions() -> StreamingResponse:
    """导出最终调度执行记录，含每个被合并周期的执行状态。"""
    items = catchup_service.list_executions()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    header = ["执行编号", "批次编号", "任务编号", "窗口周期", "执行状态", "开始时间", "结束时间", "是否归档"]
    writer.writerow(header)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in header[:-1]] + ["是" if item.get("是否归档") else "否"])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=scheduler_executions.csv"},
    )
