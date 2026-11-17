"""补跑窗口合并与调度状态推进服务。

设计要点（对应本轮优化目标）：

1. 来源记录（``scheduler_missed_sources``）是核对输入，离线恢复对同一任务同一周期的
   多次重复上报原样保留；核对时按「来源优先级 → 最晚记录时间」裁决，冲突后仍产出唯一
   核对条目（版本 + 任务 + 周期唯一）。
2. 核对与窗口合并在同一次提交（同一推进边界）内完成：一次提交只生成一份有效版本
   （``is_current=True``），旧版本自动转历史。
3. 相邻错过窗口合并成一个补跑批次；同一任务同一周期在有效批次/未归档执行记录中唯一，
   已覆盖周期不再进入新批次 —— 从数据上保证同一周期不会连续重复执行、队列不积压已完成周期。
4. 批次状态变更（开始/完成/失败/归档）同步写执行记录与生命周期事件，前后可查。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.store import store

MISSED_TABLE = "scheduler_missed_sources"
VERSION_TABLE = "scheduler_catchup_versions"
ITEM_TABLE = "scheduler_catchup_items"
BATCH_TABLE = "scheduler_catchup_batches"
EXECUTION_TABLE = "scheduler_executions"
EVENT_TABLE = "scheduler_catchup_events"
TASK_TABLE = "scheduler"

BATCH_STATUSES = ["待补跑", "补跑中", "已完成", "执行失败", "已归档"]
EXECUTION_STATUSES = ["待执行", "执行中", "已完成", "执行失败"]
# 仍占用周期、不允许再次补跑的批次状态
ACTIVE_BATCH_STATUSES = {"待补跑", "补跑中", "执行失败"}
MERGE_SCOPES = {"task", "group"}


def _next_id(table: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(table)), default=0) + 1


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _sorted_periods(periods: set[str] | list[str]) -> list[str]:
    return sorted(periods)


def _period_step(period: str, cadence: str) -> str:
    """按任务执行周期（daily/weekly）推进一个周期，用于相邻窗口判定。"""
    year, month, day = (int(part) for part in period.split("-"))
    base = datetime(year, month, day)
    step = timedelta(days=7) if cadence == "weekly" else timedelta(days=1)
    return (base + step).date().isoformat()


class CatchupService:
    # ------------------------------------------------------------------ 取数
    def list_sources(self, task_code: str | None = None, source: str | None = None) -> list[dict[str, Any]]:
        """读取补跑窗口来源记录（核对输入），支持跨系统模块按来源筛选。"""
        rows = store.rows(MISSED_TABLE)
        result = rows
        if task_code:
            result = [r for r in result if r.get("任务编号") == task_code]
        if source:
            result = [r for r in result if r.get("来源系统") == source]
        return [dict(r) for r in result]

    def list_versions(self) -> list[dict[str, Any]]:
        return [dict(r) for r in store.rows(VERSION_TABLE)]

    def current_version(self) -> dict[str, Any] | None:
        for row in store.rows(VERSION_TABLE):
            if row.get("is_current"):
                return dict(row)
        return None

    def list_items(self, version: str | None = None, task_code: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(ITEM_TABLE)
        if version is None:
            current = self.current_version()
            version = str(current["版本号"]) if current else None
        result = rows
        if version:
            result = [r for r in result if r.get("版本号") == version]
        if task_code:
            result = [r for r in result if r.get("任务编号") == task_code]
        return [dict(r) for r in result]

    def list_batches(self, status: str | None = None, task_code: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(BATCH_TABLE)
        result = rows
        if status:
            result = [r for r in result if r.get("状态") == status]
        if task_code:
            result = [r for r in result if r.get("任务编号") == task_code]
        return [dict(r) for r in result]

    def list_executions(self, task_code: str | None = None, include_archived: bool = True) -> list[dict[str, Any]]:
        rows = store.rows(EXECUTION_TABLE)
        result = rows
        if not include_archived:
            result = [r for r in result if not r.get("是否归档")]
        if task_code:
            result = [r for r in result if r.get("任务编号") == task_code]
        return [dict(r) for r in result]

    def list_events(self, batch_no: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(EVENT_TABLE)
        if batch_no:
            rows = [r for r in rows if r.get("批次编号") == batch_no]
        return [dict(r) for r in rows]

    def queue_summary(self) -> dict[str, Any]:
        """积压队列视图：只统计仍占用周期的有效批次，已完成周期不再留在队列里。"""
        batches = store.rows(BATCH_TABLE)
        queued = [b for b in batches if b.get("状态") in ACTIVE_BATCH_STATUSES]
        pending_periods = sum(len(b.get("窗口周期清单", [])) for b in queued)
        return {
            "积压批次数": len(queued),
            "积压周期数": pending_periods,
            "待补跑批次数": sum(1 for b in queued if b.get("状态") == "待补跑"),
            "补跑中批次数": sum(1 for b in queued if b.get("状态") == "补跑中"),
            "失败批次数": sum(1 for b in queued if b.get("状态") == "执行失败"),
            "已完成批次数": sum(1 for b in batches if b.get("状态") == "已完成"),
            "执行记录数": len(store.rows(EXECUTION_TABLE)),
        }

    # ----------------------------------------------------------- 来源登记
    def ingest_source(self, task_code: str, period: str, source: str,
                      priority: int, recorded_at: str | None = None) -> dict[str, Any]:
        """登记一条跨系统模块上报的错过窗口来源记录（核对输入，不去重、原样保留）。"""
        row = {
            "id": _next_id(MISSED_TABLE),
            "任务编号": task_code,
            "窗口周期": period,
            "来源系统": source,
            "来源优先级": priority,
            "记录时间": recorded_at or _now(),
            "状态": "待核对",
        }
        store.rows(MISSED_TABLE).append(row)
        return dict(row)

    # ----------------------------------------------------------- 核对与合并
    def reconcile(self, merge_scope: str = "task", remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """读取当前来源记录 → 冲突核对 → 窗口合并 → 推进调度状态，在一个推进边界内完成。

        返回新版本摘要；若没有可合并的新窗口则不产生新版本（幂等）。
        """
        if merge_scope not in MERGE_SCOPES:
            return None, f"合并范围仅支持 {sorted(MERGE_SCOPES)}"

        # 仅核对尚未消费的来源记录；已核对来源保留可查但不再参与合并，
        # 避免批次归档后旧来源被重新拾起造成重复补跑
        sources = [r for r in store.rows(MISSED_TABLE) if r.get("状态") != "已核对"]
        if not sources:
            return None, "没有待核对的补跑窗口来源记录"

        # 1) 冲突核对：同任务同周期的多条来源裁决为唯一核对条目
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in sources:
            grouped.setdefault((str(row["任务编号"]), str(row["窗口周期"])), []).append(row)

        reconciled: list[dict[str, Any]] = []
        conflict_count = 0
        for (task_code, period), records in sorted(grouped.items()):
            winner = max(records, key=lambda r: (int(r.get("来源优先级", 0)), str(r.get("记录时间", ""))))
            conflict = len(records) > 1
            if conflict:
                conflict_count += 1
            reconciled.append({
                "任务编号": task_code,
                "窗口周期": period,
                "采用来源": winner.get("来源系统"),
                "来源记录": sorted(int(r.get("id", 0)) for r in records),
                "冲突": conflict,
                "裁决说明": self._conflict_note(records, winner),
            })

        # 2) 排除已被有效批次或未归档执行记录覆盖的周期，避免同一周期重复补跑
        occupied = self._occupied_periods()
        fresh = [item for item in reconciled
                 if item["窗口周期"] not in occupied.get(item["任务编号"], set())]
        if merge_scope == "group":
            # 依赖组整体合并：先求组内新窗口并集，再向同组任务扩散，最后逐任务剔除已占用周期
            group_union: dict[str, set[str]] = {}
            period_sources: dict[str, list[int]] = {}
            for item in fresh:
                period_sources.setdefault(item["窗口周期"], []).extend(item["来源记录"])
                group = self._task_group(item["任务编号"])
                if group:
                    group_union.setdefault(group, set()).add(item["窗口周期"])
            group_tasks: dict[str, list[str]] = {}
            for task in store.rows(TASK_TABLE):
                group = str(task.get("依赖关系", "") or "")
                if group:
                    group_tasks.setdefault(group, []).append(str(task["任务编号"]))
            expanded: list[dict[str, Any]] = []
            for group, periods in group_union.items():
                for task_code in group_tasks.get(group, []):
                    for period in sorted(periods - occupied.get(task_code, set())):
                        expanded.append({
                            "任务编号": task_code,
                            "窗口周期": period,
                            "采用来源": "group-merge",
                            "来源记录": sorted(set(period_sources.get(period, []))),
                            "冲突": False,
                            "裁决说明": f"依赖组 {group} 整体合并：组内窗口并集扩散到同组任务",
                        })
            # 无依赖组的任务仍按各自新窗口处理
            expanded.extend(item for item in fresh if not self._task_group(item["任务编号"]))
            fresh = expanded
        if not fresh:
            return None, "来源窗口均已被有效批次或执行记录覆盖，无需重复补跑"

        # 3) 生成唯一有效版本（旧版本转历史，一次提交只保留一份有效版本）
        previous = self.current_version()
        next_version_no = self._next_version_no()
        version_row = {
            "id": _next_id(VERSION_TABLE),
            "版本号": next_version_no,
            "生成时间": _now(),
            "is_current": True,
            "输入来源数": len(sources),
            "核对条目数": len(reconciled),
            "冲突条目数": conflict_count,
            "合并范围": merge_scope,
            "备注": remark or f"核对 {len(sources)} 条来源，合并 {len(fresh)} 个新窗口",
        }
        for row in store.rows(VERSION_TABLE):
            row["is_current"] = False
        store.rows(VERSION_TABLE).append(version_row)

        # 4) 写入完整核对明细（版本内 任务+周期 唯一），本版新窗口标记待补跑
        fresh_keys = {(item["任务编号"], item["窗口周期"]) for item in fresh}
        for item in reconciled:
            status = "待补跑" if (item["任务编号"], item["窗口周期"]) in fresh_keys else "已合并"
            store.rows(ITEM_TABLE).append({
                "id": _next_id(ITEM_TABLE),
                "版本号": next_version_no,
                "状态": status,
                **item,
            })
        # group 范围下组内扩散产生的窗口不在原始来源中，单独补写核对明细
        for item in fresh:
            if (item["任务编号"], item["窗口周期"]) not in {
                (r["任务编号"], r["窗口周期"]) for r in reconciled
            }:
                store.rows(ITEM_TABLE).append({
                    "id": _next_id(ITEM_TABLE),
                    "版本号": next_version_no,
                    "状态": "待补跑",
                    **item,
                })

        # 5) 相邻窗口合并成批次，并在同一推进边界推进状态
        created_batches = self._merge_into_batches(fresh, merge_scope, next_version_no)
        for row in sources:
            row["状态"] = "已核对"

        return {
            "version": dict(version_row),
            "previous_version": previous.get("版本号") if previous else None,
            "reconciled_items": len(reconciled),
            "conflict_items": conflict_count,
            "merged_windows": len(fresh),
            "created_batches": [b["批次编号"] for b in created_batches],
        }, "核对与窗口合并完成"

    def _conflict_note(self, records: list[dict[str, Any]], winner: dict[str, Any]) -> str:
        if len(records) == 1:
            return "单一来源直接采用"
        systems = sorted({str(r.get("来源系统")) for r in records})
        return (
            f"来源 {'、'.join(systems)} 对同一周期重复上报 {len(records)} 次；"
            f"按来源优先级裁决采用 {winner.get('来源系统')}，重复记录去重保留"
        )

    def _next_version_no(self) -> str:
        existing = [str(r.get("版本号", "")) for r in store.rows(VERSION_TABLE)]
        nums = [int(v[1:]) for v in existing if v.startswith("v") and v[1:].isdigit()]
        return f"v{max(nums, default=0) + 1}"

    def _occupied_periods(self) -> dict[str, set[str]]:
        """当前仍占用周期的集合：所有已建批次的窗口 + 未归档执行记录的窗口。

        已归档批次代表该周期已经成功执行并留存历史，同样不允许再次补跑；
        否则归档后同周期来源会重新进入队列，重新引入重复执行。
        """
        occupied: dict[str, set[str]] = {}
        for batch in store.rows(BATCH_TABLE):
            occupied.setdefault(str(batch["任务编号"]), set()).update(batch.get("窗口周期清单", []))
        for execution in store.rows(EXECUTION_TABLE):
            if not execution.get("是否归档"):
                occupied.setdefault(str(execution["任务编号"]), set()).add(str(execution["窗口周期"]))
        return occupied

    def _task_cadence(self, task_code: str) -> str:
        for task in store.rows(TASK_TABLE):
            if task.get("任务编号") == task_code:
                return str(task.get("执行周期", "daily"))
        return "daily"

    def _task_group(self, task_code: str) -> str:
        for task in store.rows(TASK_TABLE):
            if task.get("任务编号") == task_code:
                return str(task.get("依赖关系", "") or "")
        return ""

    def _merge_into_batches(self, items: list[dict[str, Any]], merge_scope: str, version_no: str) -> list[dict[str, Any]]:
        """把核对后的窗口按相邻周期合并为批次。

        - task：每个任务独立合并相邻窗口；
        - group：同一依赖组先求组内窗口并集，再按任务落成批次（保留任务维度的执行记录，
          但组内任务共享同一合并边界，批次编号串联）。
        """
        by_task: dict[str, list[str]] = {}
        for item in items:
            by_task.setdefault(item["任务编号"], []).append(item["窗口周期"])

        # group 范围的组内窗口扩散与已占用剔除已在核对阶段完成，这里只负责相邻窗口切分

        created: list[dict[str, Any]] = []
        for task_code in sorted(by_task):
            periods = _sorted_periods(set(by_task[task_code]))
            cadence = self._task_cadence(task_code)
            # 相邻周期切分：与上一周期步进值不连续则另起一批
            segments: list[list[str]] = []
            for period in periods:
                if segments and _period_step(segments[-1][-1], cadence) == period:
                    segments[-1].append(period)
                else:
                    segments.append([period])

            group = self._task_group(task_code)
            for segment in segments:
                batch = {
                    "id": _next_id(BATCH_TABLE),
                    "批次编号": self._next_batch_no(),
                    "任务编号": task_code,
                    "依赖组": group,
                    "合并范围": merge_scope,
                    "窗口周期清单": segment,
                    "合并来源版本": version_no,
                    "状态": "待补跑",
                    "创建时间": _now(),
                    "更新时间": _now(),
                    "执行次数": 0,
                }
                store.rows(BATCH_TABLE).append(batch)
                self._add_event(batch["批次编号"], task_code, "批次创建", "", "待补跑",
                                f"{version_no} 核对后合并 {len(segment)} 个相邻窗口")
                created.append(batch)
        return created

    def _next_batch_no(self) -> str:
        return f"CB-{len(store.rows(BATCH_TABLE)) + 1:04d}"

    def _next_execution_no(self) -> str:
        return f"EX-{len(store.rows(EXECUTION_TABLE)) + 1:04d}"

    def _find_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(BATCH_TABLE):
            if row.get("批次编号") == batch_no:
                return row
        return None

    def _add_event(self, batch_no: str, task_code: str, event_type: str, before: str, after: str, note: str) -> None:
        store.rows(EVENT_TABLE).append({
            "id": _next_id(EVENT_TABLE),
            "批次编号": batch_no,
            "任务编号": task_code,
            "事件类型": event_type,
            "前状态": before,
            "后状态": after,
            "事件时间": _now(),
            "说明": note,
        })

    # ------------------------------------------------------------- 状态推进
    def start_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        batch = self._find_batch(batch_no)
        if batch is None:
            return None, f"补跑批次 {batch_no} 不存在或已归档"
        if batch["状态"] not in {"待补跑", "执行失败"}:
            return None, f"批次当前状态为「{batch['状态']}」，不能开始补跑"
        before = batch["状态"]
        batch["状态"] = "补跑中"
        batch["更新时间"] = _now()
        self._add_event(batch_no, str(batch["任务编号"]), "开始补跑", before, "补跑中", "推进边界内一次性执行合并窗口")
        # 为批次内每个窗口预占执行记录（待执行），同任务同周期唯一约束在写入处兜底
        for period in batch["窗口周期清单"]:
            self._create_execution(batch, period, "待执行")
        return dict(batch), "补跑已开始"

    def complete_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        batch = self._find_batch(batch_no)
        if batch is None:
            return None, f"补跑批次 {batch_no} 不存在或已归档"
        if batch["状态"] != "补跑中":
            return None, f"批次当前状态为「{batch['状态']}」，只有补跑中批次可完成"
        stamp = _now()
        for execution in store.rows(EXECUTION_TABLE):
            if execution.get("批次编号") == batch_no and not execution.get("是否归档"):
                execution["执行状态"] = "已完成"
                execution["结束时间"] = stamp
        batch["状态"] = "已完成"
        batch["执行次数"] = int(batch.get("执行次数", 0)) + 1
        batch["更新时间"] = stamp
        # 同一推进边界推进任务状态并清除积压：窗口已完成，队列不再保留这些周期
        self._advance_task_state(str(batch["任务编号"]))
        self._add_event(batch_no, str(batch["任务编号"]), "补跑完成", "补跑中", "已完成",
                        f"{len(batch['窗口周期清单'])} 个窗口全部执行成功，队列同步清除")
        return dict(batch), "补跑完成，合并窗口已从积压队列清除"

    def fail_batch(self, batch_no: str, reason: str | None = None) -> tuple[dict[str, Any] | None, str]:
        batch = self._find_batch(batch_no)
        if batch is None:
            return None, f"补跑批次 {batch_no} 不存在或已归档"
        if batch["状态"] != "补跑中":
            return None, f"批次当前状态为「{batch['状态']}」，不能标记失败"
        stamp = _now()
        for execution in store.rows(EXECUTION_TABLE):
            if execution.get("批次编号") == batch_no and not execution.get("是否归档"):
                execution["执行状态"] = "执行失败"
                execution["结束时间"] = stamp
        batch["状态"] = "执行失败"
        batch["执行次数"] = int(batch.get("执行次数", 0)) + 1
        batch["更新时间"] = stamp
        self._add_event(batch_no, str(batch["任务编号"]), "补跑失败", "补跑中", "执行失败",
                        reason or "窗口执行失败，批次保留待重试，不产生重复周期")
        return dict(batch), "批次已标记失败，可在原批次上重试，不会复制窗口"

    def archive_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        batch = self._find_batch(batch_no)
        if batch is None:
            return None, f"补跑批次 {batch_no} 不存在"
        if batch["状态"] != "已完成":
            return None, f"批次当前状态为「{batch['状态']}」，只有已完成批次可归档"
        before = batch["状态"]
        batch["状态"] = "已归档"
        batch["更新时间"] = _now()
        for execution in store.rows(EXECUTION_TABLE):
            if execution.get("批次编号") == batch_no:
                execution["是否归档"] = True
        self._add_event(batch_no, str(batch["任务编号"]), "批次归档", before, "已归档",
                        "执行记录同步归档，生命周期留痕可查")
        return dict(batch), "批次与执行记录已归档"

    def _create_execution(self, batch: dict[str, Any], period: str, status: str) -> dict[str, Any]:
        task_code = str(batch["任务编号"])
        # 唯一约束兜底：同任务同周期的未归档执行记录已存在则复用，不再新增
        for execution in store.rows(EXECUTION_TABLE):
            if (execution.get("任务编号") == task_code and execution.get("窗口周期") == period
                    and not execution.get("是否归档")):
                return execution
        row = {
            "id": _next_id(EXECUTION_TABLE),
            "执行编号": self._next_execution_no(),
            "批次编号": batch["批次编号"],
            "任务编号": task_code,
            "窗口周期": period,
            "执行状态": status,
            "开始时间": _now(),
            "结束时间": None,
            "是否归档": False,
        }
        store.rows(EXECUTION_TABLE).append(row)
        return row

    def _advance_task_state(self, task_code: str) -> None:
        """批次完成后在同一推进边界推进调度任务状态。"""
        for task in store.rows(TASK_TABLE):
            if task.get("任务编号") == task_code:
                task["status"] = "已完成"
                task["任务状态"] = "已完成"
                task["pending"] = False
                task["abnormal"] = False
                break

    # ------------------------------------------------------------- 一致性核对
    def check_integrity(self) -> dict[str, Any]:
        """验收用：检查同一周期是否存在连续重复执行等数据冲突。"""
        problems: list[str] = []

        # 1) 所有批次中同任务同周期唯一（含已归档，归档历史同样不允许重复）
        seen: dict[tuple[str, str], str] = {}
        for batch in store.rows(BATCH_TABLE):
            for period in batch.get("窗口周期清单", []):
                key = (str(batch["任务编号"]), str(period))
                if key in seen:
                    problems.append(f"周期 {key} 同时出现在批次 {seen[key]} 与 {batch['批次编号']}")
                seen[key] = str(batch["批次编号"])

        # 2) 全部执行记录中同任务同周期唯一
        exec_seen: dict[tuple[str, str], str] = {}
        for execution in store.rows(EXECUTION_TABLE):
            key = (str(execution["任务编号"]), str(execution["窗口周期"]))
            if key in exec_seen:
                problems.append(f"周期 {key} 存在重复执行记录 {exec_seen[key]} 与 {execution['执行编号']}")
            exec_seen[key] = str(execution["执行编号"])

        # 3) 有效版本唯一
        currents = [v["版本号"] for v in store.rows(VERSION_TABLE) if v.get("is_current")]
        if len(currents) > 1:
            problems.append(f"存在多个有效核对版本：{currents}")

        # 4) 当前版本内 任务+周期 唯一
        current = self.current_version()
        if current:
            item_seen: set[tuple[str, str]] = set()
            for item in store.rows(ITEM_TABLE):
                if item.get("版本号") != current["版本号"]:
                    continue
                key = (str(item["任务编号"]), str(item["窗口周期"]))
                if key in item_seen:
                    problems.append(f"版本 {current['版本号']} 内周期 {key} 核对结果不唯一")
                item_seen.add(key)

        return {"ok": not problems, "problems": problems, "checked_at": _now()}


catchup_service = CatchupService()
