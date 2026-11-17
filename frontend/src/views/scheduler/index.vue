<template>
  <section class="page" data-module="scheduler">
    <header class="page-head">
      <div><h2>任务调度</h2><p class="page-desc">维护调度任务，围绕任务编号、任务名称、调度类型、执行周期做登记、筛选与状态流转；补跑窗口在同一推进边界内核对合并，避免同一周期连续重复执行。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记调度任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <!-- 补跑窗口合并与状态推进 -->
    <div class="catchup-panel" data-testid="catchup-panel">
      <div class="catchup-head">
        <h3>补跑窗口合并与调度状态推进</h3>
        <div class="catchup-controls">
          <label><span>合并范围</span>
            <select v-model="mergeScope" data-testid="merge-scope">
              <option value="task">按任务分别合并</option>
              <option value="group">按依赖组整体合并</option>
            </select>
          </label>
          <button class="btn primary" type="button" data-testid="reconcile" @click="runReconcile">读取来源并核对合并</button>
          <button class="btn" type="button" data-testid="export-batches" @click="exportBatches">导出补跑批次</button>
          <button class="btn" type="button" data-testid="export-executions" @click="exportExecutions">导出执行记录</button>
        </div>
      </div>
      <p v-if="catchupMessage" class="catchup-msg" :class="{ error: !catchupOk }" data-testid="catchup-message">{{ catchupMessage }}</p>

      <div class="kpi-row">
        <div class="kpi-card" data-testid="kpi-queued-batches"><strong>{{ queue['积压批次数'] ?? 0 }}</strong><span>积压批次</span></div>
        <div class="kpi-card" data-testid="kpi-queued-periods"><strong>{{ queue['积压周期数'] ?? 0 }}</strong><span>积压周期</span></div>
        <div class="kpi-card"><strong>{{ queue['补跑中批次数'] ?? 0 }}</strong><span>补跑中</span></div>
        <div class="kpi-card"><strong>{{ queue['失败批次数'] ?? 0 }}</strong><span>失败批次</span></div>
        <div class="kpi-card"><strong>{{ queue['已完成批次数'] ?? 0 }}</strong><span>已完成批次</span></div>
        <div class="kpi-card" :class="{ bad: !integrityOk }" data-testid="kpi-integrity"><strong>{{ integrityOk ? '通过' : '异常' }}</strong><span>唯一性核对</span></div>
        <div class="kpi-card"><strong>{{ currentVersion || '—' }}</strong><span>有效核对版本</span></div>
      </div>

      <h4>合并后的补跑批次（含被合并周期与最终执行状态）</h4>
      <table class="data-table catchup-table" data-testid="batch-table">
        <thead>
          <tr><th>批次编号</th><th>任务编号</th><th>依赖组</th><th>合并范围</th><th>被合并周期</th><th>周期数</th><th>来源版本</th><th>状态</th><th>执行次数</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b['批次编号']" :data-status="b['状态']" :data-testid="`batch-row-${b['批次编号']}`">
            <td>{{ b['批次编号'] }}</td>
            <td>{{ b['任务编号'] }}</td>
            <td>{{ b['依赖组'] || '—' }}</td>
            <td>{{ b['合并范围'] === 'group' ? '依赖组整体' : '按任务' }}</td>
            <td class="period-cell">{{ b['窗口周期清单'].join('、') }}</td>
            <td>{{ b['窗口周期清单'].length }}</td>
            <td>{{ b['合并来源版本'] }}</td>
            <td><span class="status-tag" :class="`tag-${b['状态']}`">{{ b['状态'] }}</span></td>
            <td>{{ b['执行次数'] }}</td>
            <td class="action-cell">
              <button v-if="b['状态'] === '待补跑' || b['状态'] === '执行失败'" class="btn" type="button" @click="batchAction(b['批次编号'], 'start')">开始补跑</button>
              <button v-if="b['状态'] === '补跑中'" class="btn primary" type="button" @click="batchAction(b['批次编号'], 'complete')">补跑完成</button>
              <button v-if="b['状态'] === '补跑中'" class="btn" type="button" @click="batchAction(b['批次编号'], 'fail')">标记失败</button>
              <button v-if="b['状态'] === '已完成'" class="btn" type="button" @click="batchAction(b['批次编号'], 'archive')">归档</button>
              <button class="btn ghost" type="button" @click="toggleEvents(b['批次编号'])">生命周期</button>
            </td>
          </tr>
          <tr v-if="!batches.length"><td colspan="10" style="text-align:center;color:var(--muted)">暂无补跑批次</td></tr>
        </tbody>
      </table>
      <div v-if="activeEvents.length" class="event-box" data-testid="event-box">
        <strong>{{ eventsBatchNo }} 生命周期流水</strong>
        <ul>
          <li v-for="e in activeEvents" :key="e.id">{{ e['事件时间'] }}｜{{ e['事件类型'] }}｜{{ e['前状态'] || '—' }} → {{ e['后状态'] }}｜{{ e['说明'] }}</li>
        </ul>
      </div>

      <h4>数据合并后的调度执行记录（同一任务同一周期唯一）</h4>
      <table class="data-table catchup-table" data-testid="execution-table">
        <thead>
          <tr><th>执行编号</th><th>批次编号</th><th>任务编号</th><th>窗口周期</th><th>执行状态</th><th>开始时间</th><th>结束时间</th><th>归档</th></tr>
        </thead>
        <tbody>
          <tr v-for="e in executions" :key="e['执行编号']" :data-testid="`exec-row-${e['执行编号']}`">
            <td>{{ e['执行编号'] }}</td>
            <td>{{ e['批次编号'] }}</td>
            <td>{{ e['任务编号'] }}</td>
            <td>{{ e['窗口周期'] }}</td>
            <td><span class="status-tag" :class="`tag-${e['执行状态']}`">{{ e['执行状态'] }}</span></td>
            <td>{{ e['开始时间'] || '—' }}</td>
            <td>{{ e['结束时间'] || '—' }}</td>
            <td>{{ e['是否归档'] ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!executions.length"><td colspan="8" style="text-align:center;color:var(--muted)">暂无执行记录</td></tr>
        </tbody>
      </table>

      <h4>补跑窗口清单来源记录（核对输入，重复上报原样保留）</h4>
      <table class="data-table catchup-table" data-testid="source-table">
        <thead>
          <tr><th>任务编号</th><th>窗口周期</th><th>来源系统</th><th>优先级</th><th>记录时间</th><th>核对状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="src in sources" :key="src.id">
            <td>{{ src['任务编号'] }}</td>
            <td>{{ src['窗口周期'] }}</td>
            <td>{{ src['来源系统'] }}</td>
            <td>{{ src['来源优先级'] }}</td>
            <td>{{ src['记录时间'] }}</td>
            <td>{{ src['状态'] }}</td>
          </tr>
          <tr v-if="!sources.length"><td colspan="6" style="text-align:center;color:var(--muted)">暂无来源记录</td></tr>
        </tbody>
      </table>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记调度任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/scheduler'
const CATCHUP = '/api/scheduler/catchup'
const columns = ["任务编号", "任务名称", "调度类型", "执行周期", "上次执行", "下次执行", "依赖关系", "任务状态"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])

const mergeScope = ref<'task' | 'group'>('task')
const batches = ref<any[]>([])
const executions = ref<any[]>([])
const sources = ref<any[]>([])
const queue = ref<Record<string, number>>({})
const currentVersion = ref('')
const integrityOk = ref(true)
const catchupMessage = ref(''); const catchupOk = ref(true)
const activeEvents = ref<any[]>([]); const eventsBatchNo = ref('')

function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

async function loadCatchup() {
  try {
    const [batchRes, execRes, sourceRes, versionRes, integrityRes] = await Promise.all([
      request(`${CATCHUP}/batches`), request(`${CATCHUP}/executions`),
      request(`${CATCHUP}/sources`), request(`${CATCHUP}/versions`), request(`${CATCHUP}/integrity`),
    ])
    const batchData = await batchRes.json()
    batches.value = batchData.items || []
    queue.value = batchData.queue || {}
    const execData = await execRes.json(); executions.value = execData.items || []
    const sourceData = await sourceRes.json(); sources.value = sourceData.items || []
    const versionData = await versionRes.json()
    currentVersion.value = versionData.current ? versionData.current['版本号'] : ''
    const integrityData = await integrityRes.json(); integrityOk.value = !!integrityData.ok
  } catch (e: any) {
    catchupOk.value = false; catchupMessage.value = e.message || '补跑数据加载失败'
  }
}

function setMessage(ok: boolean, message: string) {
  catchupOk.value = ok; catchupMessage.value = message
}

async function runReconcile() {
  try {
    const res = await request(`${CATCHUP}/reconcile`, {
      method: 'POST', body: JSON.stringify({ merge_scope: mergeScope.value }),
    })
    const data = await res.json()
    setMessage(!!data.ok, data.message || '核对完成')
    await loadCatchup()
  } catch (e: any) {
    setMessage(false, e.message)
  }
}

async function batchAction(batchNo: string, action: 'start' | 'complete' | 'fail' | 'archive') {
  try {
    const res = await request(`${CATCHUP}/batches/${batchNo}/${action}`, {
      method: 'POST', body: JSON.stringify({}),
    })
    const data = await res.json()
    setMessage(!!data.ok, data.message || '操作完成')
    await loadCatchup()
  } catch (e: any) {
    setMessage(false, e.message)
  }
}

async function toggleEvents(batchNo: string) {
  if (eventsBatchNo.value === batchNo && activeEvents.value.length) {
    eventsBatchNo.value = ''; activeEvents.value = []; return
  }
  const res = await request(`${CATCHUP}/events?batch_no=${encodeURIComponent(batchNo)}`)
  const data = await res.json()
  eventsBatchNo.value = batchNo; activeEvents.value = data.items || []
}

function exportBatches() { window.open(`${CATCHUP}/export/batches`, '_blank') }
function exportExecutions() { window.open(`${CATCHUP}/export/executions`, '_blank') }

onMounted(() => { reload(); loadCatchup() })
</script>
<style scoped>
.catchup-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
.catchup-head { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
.catchup-head h3 { margin: 0; font-size: 16px; }
.catchup-controls { display: flex; gap: 8px; align-items: flex-end; flex-wrap: wrap; }
.catchup-controls label { display: flex; flex-direction: column; font-size: 12px; color: var(--muted); gap: 4px; }
.catchup-msg { padding: 8px 12px; border-radius: 6px; background: #ecfdf5; color: #047857; font-size: 13px; }
.catchup-msg.error { background: #fef2f2; color: #b91c1c; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 14px 0; }
.kpi-card { border: 1px solid var(--border); border-radius: 10px; padding: 10px; display: flex; flex-direction: column; gap: 4px; background: #f8fafc; }
.kpi-card strong { font-size: 20px; }
.kpi-card span { font-size: 12px; color: var(--muted); }
.kpi-card.bad { border-color: #fca5a5; background: #fef2f2; }
.catchup-table { margin: 8px 0 18px; }
.catchup-table th, .catchup-table td { white-space: nowrap; }
.catchup-table .period-cell, .catchup-table .action-cell { white-space: normal; }
.status-tag { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #e2e8f0; color: #334155; }
.tag-已完成 { background: #dcfce7; color: #166534; }
.tag-补跑中, .tag-执行中 { background: #dbeafe; color: #1d4ed8; }
.tag-执行失败 { background: #fee2e2; color: #b91c1c; }
.tag-待补跑, .tag-待执行 { background: #fef9c3; color: #a16207; }
.tag-已归档 { background: #f1f5f9; color: #64748b; }
.event-box { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 16px; background: #f8fafc; }
.event-box ul { margin: 8px 0 0; padding-left: 18px; font-size: 12px; color: var(--muted); }
.event-box li { margin-bottom: 4px; }
h4 { font-size: 14px; margin: 12px 0 6px; }
</style>
