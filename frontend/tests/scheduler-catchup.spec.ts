import { expect, test } from '@playwright/test'

const BASE = 'http://127.0.0.1:5173'

test('调度页：补跑面板渲染、核对合并、状态推进、唯一性与导出', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', e => errors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

  await page.goto(`${BASE}/scheduler`)

  // 既有时间线能力保留
  await expect(page.locator('.timeline-bar').first()).toBeVisible()
  expect(await page.locator('.timeline-bar').count()).toBeGreaterThanOrEqual(3)

  // 新补跑面板
  await expect(page.getByTestId('catchup-panel')).toBeVisible()
  await expect(page.getByTestId('batch-table')).toBeVisible()
  await expect(page.getByTestId('execution-table')).toBeVisible()
  await expect(page.getByTestId('source-table')).toBeVisible()

  // 初始唯一性核对通过
  await expect(page.getByTestId('kpi-integrity')).toContainText('通过')
  // 当前有效版本存在且唯一展示
  await expect(page.getByTestId('kpi-integrity')).toBeVisible()

  const initialBatchRows = page.locator('[data-testid^="batch-row-"]')
  const initialCount = await initialBatchRows.count()
  expect(initialCount).toBeGreaterThanOrEqual(3)

  // 批次行内被合并周期可见
  const firstPeriodCell = page.locator('[data-testid^="batch-row-"]').first().locator('.period-cell')
  await expect(firstPeriodCell).not.toBeEmpty()

  // 上报同周期重复来源 + 相邻窗口，再核对合并
  await page.evaluate(async () => {
    const post = (body: unknown) => fetch('/api/scheduler/catchup/sources', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    })
    await post({ task_code: 'SCHE-0002', period: '2026-11-30', source: 'offline-recovery', priority: 2 })
    await post({ task_code: 'SCHE-0002', period: '2026-11-30', source: 'offline-recovery', priority: 2 })
    await post({ task_code: 'SCHE-0002', period: '2026-11-30', source: 'scheduler-engine', priority: 3 })
  })
  await page.getByTestId('reconcile').click()
  await expect(page.getByTestId('catchup-message')).toContainText('核对与窗口合并完成')

  // 出现新批次（同一周期只合并成一个批次，不会出现两行相同周期批次）
  await expect.poll(async () => page.locator('[data-testid^="batch-row-"]').count()).toBeGreaterThan(initialCount)
  const periodTexts = await page.locator('[data-testid^="batch-row-"] .period-cell').allInnerTexts()
  const allPeriods = periodTexts.flatMap(text => text.split('、').map(s => s.trim()))
  expect(allPeriods.filter(p => p === '2026-11-30')).toHaveLength(1)

  // 再次核对应为幂等提示且不新增批次
  const countAfterMerge = await page.locator('[data-testid^="batch-row-"]').count()
  await page.getByTestId('reconcile').click()
  await expect(page.getByTestId('catchup-message')).toContainText('没有待核对')
  expect(await page.locator('[data-testid^="batch-row-"]').count()).toBe(countAfterMerge)

  // 对新批次执行 开始 -> 完成 的状态推进
  const newRow = page.locator('[data-testid^="batch-row-"]', { hasText: '2026-11-30' }).last()
  await newRow.getByRole('button', { name: '开始补跑' }).click()
  await expect(newRow).toHaveAttribute('data-status', '补跑中')
  await newRow.getByRole('button', { name: '补跑完成' }).click()
  await expect(newRow).toHaveAttribute('data-status', '已完成')

  // 执行记录表中该周期只有一条有效记录
  const execRows = page.locator('[data-testid^="exec-row-"]', { hasText: '2026-11-30' })
  await expect.poll(async () => execRows.count()).toBe(1)
  await expect(execRows.first()).toContainText('已完成')

  // 生命周期流水可查
  await newRow.getByRole('button', { name: '生命周期' }).click()
  await expect(page.getByTestId('event-box')).toBeVisible()
  await expect(page.getByTestId('event-box')).toContainText('批次创建')
  await expect(page.getByTestId('event-box')).toContainText('补跑完成')

  // 唯一性核对仍通过
  await expect(page.getByTestId('kpi-integrity')).toContainText('通过')

  // 导出入口可用
  const downloadPromise = page.waitForEvent('download')
  await page.getByTestId('export-batches').click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('scheduler_catchup_batches.csv')

  expect(errors).toEqual([])
})

test('调度页：既有时间线登记与导出能力不回归', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', e => errors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

  await page.goto(`${BASE}/scheduler`)
  await expect(page.locator('.timeline-bar').first()).toBeVisible()

  const downloadPromise = page.waitForEvent('download')
  await page.getByTestId('export').click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('scheduler.csv')

  await page.getByTestId('open-create').click()
  await expect(page.getByTestId('create-modal')).toBeVisible()
  await page.locator('.create-field input').nth(0).fill('E2E-scheduler-1')
  await page.locator('.create-field input').nth(1).fill('E2E-scheduler-2')
  await page.locator('.create-field input').nth(2).fill('E2E-scheduler-3')
  await page.getByTestId('submit-create').click()
  await expect(page.getByTestId('create-modal')).toHaveCount(0)

  expect(errors).toEqual([])
})
