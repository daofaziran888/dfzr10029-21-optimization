"""补跑窗口合并与状态推进的端到端核对脚本（无外部依赖，直接驱动 service 层）。

运行：cd backend && .venv/bin/python tests/test_catchup_flow.py
每个场景在独立子进程中以全新内存仓库运行，避免相互污染。
"""
from __future__ import annotations

import subprocess
import sys
import textwrap


def run(scenario: str) -> None:
    code = textwrap.dedent(scenario)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd="/workspace/backend",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"场景失败:\n{result.stdout}\n{result.stderr}")
    print(result.stdout.strip())


# 场景一：重复上报去重、相邻窗口合并、状态推进、幂等、归档
SCENARIO_TASK_MERGE = """
from app.store import store
from app.services.catchup import (
    catchup_service as s, MISSED_TABLE, BATCH_TABLE, EXECUTION_TABLE, VERSION_TABLE, EVENT_TABLE,
)

def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)

# 1. 初始种子：一致性通过，唯一有效版本 v2，队列不含已完成周期
assert_true(s.check_integrity()["ok"], "初始一致性检查失败")
assert_true(s.current_version()["版本号"] == "v2", "初始有效版本应为 v2")
queue = s.queue_summary()
assert_true(queue["积压批次数"] == 2 and queue["积压周期数"] == 4, f"初始积压不符: {queue}")

# 2. 没有待核对来源时核对必须幂等无副作用
summary, msg = s.reconcile()
assert_true(summary is None, "无待核对来源不应产生新版本")
assert_true(s.current_version()["版本号"] == "v2", "幂等核对改动了有效版本")

# 3. 离线恢复对同一周期连续触发两次 + 相邻周期一次（SCHE-0002 为 weekly 节奏）
base = max(r["id"] for r in store.rows(MISSED_TABLE))
for i, (period, source, prio) in enumerate([
    ("2026-11-16", "scheduler-engine", 3),
    ("2026-11-16", "offline-recovery", 2),
    ("2026-11-16", "offline-recovery", 2),
    ("2026-11-23", "scheduler-engine", 3),
]):
    store.rows(MISSED_TABLE).append({
        "id": base + i + 1, "任务编号": "SCHE-0002", "窗口周期": period,
        "来源系统": source, "来源优先级": prio, "记录时间": f"2026-11-24T0{i}:00:00", "状态": "待核对",
    })

summary, msg = s.reconcile(merge_scope="task", remark="离线恢复重复上报核对")
assert_true(summary is not None, msg)
assert_true(summary["version"]["版本号"] == "v3", f"应生成 v3，实际 {summary}")
assert_true(summary["conflict_items"] == 1, f"应有 1 个冲突条目，实际 {summary['conflict_items']}")
assert_true(summary["merged_windows"] == 2, f"去重后应合并 2 个唯一窗口，实际 {summary['merged_windows']}")
assert_true(len(summary["created_batches"]) == 1, "weekly 相邻窗口应合并为 1 个批次")

# 4. 一次提交只保留一份有效版本，旧版本转历史
currents = [v["版本号"] for v in store.rows(VERSION_TABLE) if v["is_current"]]
assert_true(currents == ["v3"], f"有效版本不唯一: {currents}")
# 核对条目在版本内 任务+周期 唯一
v3_items = s.list_items(version="v3", task_code="SCHE-0002")
keys = [(i["任务编号"], i["窗口周期"]) for i in v3_items]
assert_true(len(keys) == len(set(keys)), "版本内核对条目不唯一")
winner = next(i for i in v3_items if i["窗口周期"] == "2026-11-16")
assert_true(winner["采用来源"] == "scheduler-engine" and winner["来源记录"].__len__() == 3,
            "冲突裁决应采用高优先级来源并保留全部来源记录 id")

# 5. 来源已标记已核对，重复核对无新版本
_, msg = s.reconcile()
assert_true("没有待核对" in msg, f"已核对来源不应再参与合并: {msg}")

# 6. 状态推进：开始 -> 失败 -> 重试（不复制执行记录）-> 完成
batch_no = summary["created_batches"][0]
b, _ = s.start_batch(batch_no)
assert_true(b["状态"] == "补跑中", "开始后应为补跑中")
exec_rows = [e for e in store.rows(EXECUTION_TABLE) if e["批次编号"] == batch_no]
assert_true(len(exec_rows) == 2, "批次内每个窗口一条执行记录")
assert_true({e["执行状态"] for e in exec_rows} == {"待执行"}, "新执行记录应为待执行")
# 重复开始应被拒绝
again, _ = s.start_batch(batch_no)
assert_true(again is None, "补跑中批次不能重复开始")
s.fail_batch(batch_no, reason="模拟下游不可用")
b, _ = s.start_batch(batch_no)
assert_true(b is not None and b["状态"] == "补跑中", "失败批次应可在原批次重试")
exec_rows2 = [e for e in store.rows(EXECUTION_TABLE) if e["批次编号"] == batch_no]
assert_true(len(exec_rows2) == 2, "重试不得复制执行记录（同一周期连续重复执行防护）")
b, _ = s.complete_batch(batch_no)
assert_true(b["状态"] == "已完成" and b["执行次数"] == 2, f"完成状态/次数不符: {b}")
assert_true(all(e["执行状态"] == "已完成" for e in exec_rows2), "执行记录应同步完成")

# 7. 完成后周期离开积压队列，任务状态在同一边界推进
queued_periods = sum(len(x["窗口周期清单"]) for x in s.list_batches(status="待补跑"))
queued_periods += sum(len(x["窗口周期清单"]) for x in s.list_batches(status="补跑中"))
assert_true(queued_periods == 4, f"完成批次的窗口不应留在队列: {queued_periods}")
task = next(t for t in store.rows("scheduler") if t["任务编号"] == "SCHE-0002")
assert_true(task["status"] == "已完成" and task["pending"] is False, "任务状态未在同一推进边界更新")

# 8. 对已覆盖周期再上报来源，核对不得产生新批次/重复执行
base = max(r["id"] for r in store.rows(MISSED_TABLE))
store.rows(MISSED_TABLE).append({
    "id": base + 1, "任务编号": "SCHE-0002", "窗口周期": "2026-11-16",
    "来源系统": "offline-recovery", "来源优先级": 2, "记录时间": "2026-11-25T00:00:00", "状态": "待核对",
})
res, msg = s.reconcile()
assert_true(res is None and "无需重复补跑" in msg, f"已覆盖周期必须拒绝重复补跑: {msg}")
dup = [e for e in store.rows(EXECUTION_TABLE)
       if e["任务编号"] == "SCHE-0002" and e["窗口周期"] == "2026-11-16" and not e["是否归档"]]
assert_true(len(dup) == 1, f"同一周期出现 {len(dup)} 条有效执行记录")

# 9. 归档：批次与执行记录同步，生命周期事件完整可查
b, _ = s.archive_batch(batch_no)
assert_true(b["状态"] == "已归档", "批次应已归档")
assert_true(all(e["是否归档"] for e in exec_rows2), "执行记录应随批次归档")
events = s.list_events(batch_no)
types = [e["事件类型"] for e in events]
for t in ["批次创建", "开始补跑", "补跑失败", "开始补跑", "补跑完成", "批次归档"]:
    assert_true(t in types, f"生命周期事件缺失 {t}: {types}")

# 10. 最终一致性：无重复周期、有效版本唯一
assert_true(s.check_integrity()["ok"], "最终一致性检查失败")
print("SCENARIO_TASK_MERGE OK")
"""

# 场景二：依赖组整体合并——组内任务共享合并边界，且与按任务合并产出不同批次结构
SCENARIO_GROUP_MERGE = """
from app.store import store
from app.services.catchup import catchup_service as s, MISSED_TABLE, BATCH_TABLE

def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)

# 新建同属 GROUP-A 的任务 SCHE-0004
tasks = store.rows("scheduler")
tasks.append({
    "id": max(t["id"] for t in tasks) + 1, "status": "待执行", "pending": True, "abnormal": False,
    "任务编号": "SCHE-0004", "任务名称": "依赖组下游宽表装载", "调度类型": "依赖触发",
    "执行周期": "daily", "上次执行": "2026-11-10", "下次执行": "2026-11-11",
    "依赖关系": "GROUP-A", "任务状态": "待执行",
})
# 只有 SCHE-0004 上报错过 11-12；组内 SCHE-0003 的 11-08/11-09 已被 CB-0003 占用
base = max(r["id"] for r in store.rows(MISSED_TABLE))
store.rows(MISSED_TABLE).append({
    "id": base + 1, "任务编号": "SCHE-0004", "窗口周期": "2026-11-12",
    "来源系统": "scheduler-engine", "来源优先级": 3, "记录时间": "2026-11-13T01:00:00", "状态": "待核对",
})

summary, msg = s.reconcile(merge_scope="group")
assert_true(summary is not None, msg)
new_batches = [b for b in store.rows(BATCH_TABLE) if b["批次编号"] in summary["created_batches"]]
group_tasks = {b["任务编号"] for b in new_batches}
assert_true(group_tasks == {"SCHE-0003", "SCHE-0004"}, f"组整体合并应覆盖组内两个任务: {group_tasks}")
for b in new_batches:
    assert_true(b["合并范围"] == "group" and b["依赖组"] == "GROUP-A", "批次应标记依赖组整体合并")
    assert_true("2026-11-12" in b["窗口周期清单"], "组内窗口应共享到同组所有任务")
# 被占用的 11-08/11-09 不得在新批次中重复出现
for b in new_batches:
    assert_true(not {"2026-11-08", "2026-11-09"} & set(b["窗口周期清单"]), "已占用周期被重复合并")
assert_true(s.check_integrity()["ok"], "组整体合并后一致性检查失败")
print("SCENARIO_GROUP_MERGE OK")
"""

# 场景三：连续多次核对提交始终只有一份有效版本；同批相邻窗口合并为单一批次
SCENARIO_VERSION_UNIQUENESS = """
from app.store import store
from app.services.catchup import catchup_service as s, MISSED_TABLE, VERSION_TABLE, BATCH_TABLE

def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)

# 三轮独立提交：每轮一个新窗口，版本号递增且有效版本始终唯一
for round_no, period in enumerate(["2026-11-15", "2026-11-16", "2026-11-17"]):
    base = max(r["id"] for r in store.rows(MISSED_TABLE))
    store.rows(MISSED_TABLE).append({
        "id": base + 1, "任务编号": "SCHE-0001", "窗口周期": period,
        "来源系统": "manual-repair", "来源优先级": 1,
        "记录时间": f"2026-11-18T0{round_no}:00:00", "状态": "待核对",
    })
    summary, _ = s.reconcile()
    assert_true(summary is not None, f"第 {round_no+1} 轮核对失败")
    currents = [v["版本号"] for v in store.rows(VERSION_TABLE) if v["is_current"]]
    assert_true(len(currents) == 1, f"第 {round_no+1} 轮出现多个有效版本: {currents}")

assert_true(s.current_version()["版本号"] == "v5", "三轮提交后有效版本应为 v5")
# 三次窗口各自成批，但同任务同周期在有效批次中唯一，且不与已完成的 CB-0001 周期冲突
keys = [(b["任务编号"], p) for b in store.rows(BATCH_TABLE)
        for p in b["窗口周期清单"] if b["状态"] != "已归档"]
assert_true(len(keys) == len(set(keys)), "有效批次中出现重复周期")
assert_true(s.check_integrity()["ok"], "多轮核对后一致性检查失败")

# 一次性提交三个相邻新窗口（SCHE-0001 为 daily），应合并为单一批次
base = max(r["id"] for r in store.rows(MISSED_TABLE))
for i, period in enumerate(["2026-11-18", "2026-11-19", "2026-11-20"]):
    store.rows(MISSED_TABLE).append({
        "id": base + i + 1, "任务编号": "SCHE-0001", "窗口周期": period,
        "来源系统": "scheduler-engine", "来源优先级": 3,
        "记录时间": f"2026-11-21T0{i}:00:00", "状态": "待核对",
    })
summary, _ = s.reconcile()
assert_true(summary is not None and len(summary["created_batches"]) == 1,
            "三个相邻日窗口应合并为一个批次")
batch = next(b for b in store.rows(BATCH_TABLE) if b["批次编号"] == summary["created_batches"][0])
assert_true(batch["窗口周期清单"] == ["2026-11-18", "2026-11-19", "2026-11-20"],
            f"合并窗口清单不符: {batch['窗口周期清单']}")
assert_true(s.check_integrity()["ok"], "相邻合并后一致性检查失败")
print("SCENARIO_VERSION_UNIQUENESS OK")
"""


if __name__ == "__main__":
    run(SCENARIO_TASK_MERGE)
    run(SCENARIO_GROUP_MERGE)
    run(SCENARIO_VERSION_UNIQUENESS)
    print("全部场景通过")
