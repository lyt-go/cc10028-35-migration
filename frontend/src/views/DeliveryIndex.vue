<template>
  <section class="page" data-testid="delivery-index">
    <header class="page-head">
      <div>
        <h2>交付索引 v2</h2>
        <p class="page-desc">
          旧快照的文档计数与交付明细在此可查；补发按「核实分片 / 仅回执 / 无回执」区分，
          交付之后更正或新增的抽取只登记为待核。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="migrate" type="button" :disabled="busy" @click="runMigrate">
          {{ busy ? '迁移中…' : '迁移旧交付' }}
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span class="legend-item">索引来源：{{ summary?.source === 'legacy_fallback' ? '旧文件兜底（回滚态）' : '索引 v2' }}</span>
      <span class="legend-item">兼容版本：{{ (summary?.compatible_versions ?? []).join(' / ') }}</span>
      <span class="legend-item">迁移：{{ summary?.migration?.status ?? '—' }}</span>
    </p>

    <form class="filter-bar" @submit.prevent="runQuery">
      <label class="filter-item">
        <span>文档编号</span>
        <input v-model="docId" data-testid="query-doc" placeholder="按文档查补发" />
      </label>
      <label class="filter-item">
        <span>快照编号</span>
        <input v-model="snapshotId" data-testid="query-snapshot" placeholder="按快照查补发" />
      </label>
      <button class="btn primary" data-testid="resend-query" type="submit">查补发</button>
      <button class="btn ghost" type="button" @click="resetQuery">重置</button>
    </form>

    <p v-if="message" class="error-text">{{ message }}</p>

    <table class="data-table" data-testid="resend-table">
      <thead>
        <tr>
          <th>快照编号</th>
          <th>版本</th>
          <th>类型</th>
          <th>交付条数</th>
          <th>回执</th>
          <th>可补发结论</th>
          <th>待核</th>
          <th>分片下载</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in entries" :key="entry.snapshot_id" :data-testid="`resend-${entry.snapshot_id}`">
          <td>{{ entry.snapshot_id }}</td>
          <td>{{ entry.version }}</td>
          <td>{{ entry.snapshot_kind === 'legacy' ? '旧快照' : 'v2 快照' }}</td>
          <td>
            {{ entry.record_count }}
            <span class="muted">（实体 {{ entry.snapshot_counts.entities ?? '—' }} / 分块 {{ entry.snapshot_counts.chunks ?? '—' }}）</span>
          </td>
          <td>{{ entry.receipt_available ? entry.receipt?.type : '无回执' }}</td>
          <td :data-status="entry.resend_status">
            {{ entry.resend_status }}
            <span class="muted">{{ entry.note }}</span>
          </td>
          <td>
            <span v-if="!entry.pending_verification.length">—</span>
            <span v-else>{{ entry.pending_verification.length }} 项待核</span>
          </td>
          <td class="row-actions">
            <button
              v-for="shard in entry.shards"
              :key="shard.shard_no"
              class="link"
              type="button"
              :disabled="!shard.downloadable"
              :data-testid="`download-${entry.snapshot_id}-${shard.shard_no}`"
              @click="onDownload(entry, shard.shard_no)"
            >
              分片 {{ shard.shard_no }}{{ shard.verified ? '（已核实）' : '（不可下载）' }}
            </button>
          </td>
        </tr>
        <tr v-if="!entries.length">
          <td colspan="8" class="empty-state">输入文档编号或快照编号查询可补发的旧结果</td>
        </tr>
      </tbody>
    </table>

    <div v-if="focused" class="detail-panel" data-testid="resend-detail">
      <h3>{{ focused.snapshot_id }} 交付明细</h3>
      <p class="muted">
        可找回成品：<span v-for="product in focused.recoverable_products" :key="product.type">
          {{ product.type }}×{{ product.records }}（分片 {{ product.shard_nos.join(',') }}）
        </span>
      </p>
      <table class="data-table">
        <thead>
          <tr><th>分片</th><th>条数</th><th>校验值</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="shard in focused.shards" :key="shard.shard_no">
            <td>{{ shard.filename }}</td>
            <td>{{ shard.records }}</td>
            <td>{{ shard.checksum || '—' }}</td>
            <td>{{ shard.verified ? '已核实' : shard.reason }}</td>
          </tr>
        </tbody>
      </table>

      <h4>回执</h4>
      <p v-if="!focused.receipts.length" class="muted">无回执记录</p>
      <ul v-else class="plain-list">
        <li v-for="receipt in focused.receipts" :key="receipt.receipt_id">
          {{ receipt.type }} · {{ receipt.receipt_id }} · {{ receipt.received_at ?? '时间未知' }}
          <button class="link" type="button" @click="onRecordReceipt(focused.snapshot_id)">补登接收回执</button>
        </li>
      </ul>

      <h4>待核归属（未经确认不计入旧版成品）</h4>
      <p v-if="!focused.pending_verification.length" class="muted">无待核项</p>
      <ul v-else class="plain-list">
        <li v-for="item in focused.pending_verification" :key="item.artifact">
          {{ item.status }}：{{ item.artifact }}（已交付 {{ item.delivered_records }} / 现场 {{ item.current_records }}）
          <span class="muted">{{ item.review_status }}</span>
          <button
            v-if="item.review_status === '待核'"
            class="link"
            type="button"
            @click="onReview(focused.snapshot_id, item.artifact, true)"
          >
            确认归属
          </button>
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  downloadShard,
  loadIndexSummary,
  migrateLegacy,
  queryResend,
  recordReceipt,
  reviewPending,
} from '@/api/delivery-index'
import type { IndexSummary, ResendEntry } from '@/data/types'

const summary = ref<IndexSummary | null>(null)
const entries = ref<ResendEntry[]>([])
const docId = ref('')
const snapshotId = ref('')
const message = ref('')
const busy = ref(false)

const focused = computed(() => entries.value[0] ?? null)

const cards = computed(() => {
  const data = summary.value
  return [
    { label: '索引快照数', value: data?.snapshot_count ?? 0 },
    { label: '旧快照', value: data?.by_kind.legacy ?? 0 },
    { label: 'v2 快照', value: data?.by_kind.v2 ?? 0 },
  ]
})

async function refreshSummary() {
  try {
    summary.value = await loadIndexSummary()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '交付索引读取失败'
  }
}

async function runMigrate() {
  busy.value = true
  message.value = ''
  try {
    await migrateLegacy()
    await refreshSummary()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '迁移失败'
  } finally {
    busy.value = false
  }
}

async function runQuery() {
  message.value = ''
  const hasFilter = docId.value.trim() || snapshotId.value.trim()
  if (!hasFilter) {
    message.value = '请先输入文档编号或快照编号'
    return
  }
  try {
    const result = await queryResend({
      docId: docId.value.trim() || undefined,
      snapshotId: snapshotId.value.trim() || undefined,
    })
    entries.value = result.entries
  } catch (error) {
    entries.value = []
    message.value = error instanceof Error ? error.message : '补发查询失败'
  }
}

function resetQuery() {
  docId.value = ''
  snapshotId.value = ''
  entries.value = []
  message.value = ''
}

function onDownload(entry: ResendEntry, shardNo: number) {
  downloadShard(entry, shardNo)
}

async function onRecordReceipt(target: string) {
  try {
    await recordReceipt(target)
    await runQuery()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '回执登记失败'
  }
}

async function onReview(target: string, artifact: string, accepted: boolean) {
  try {
    await reviewPending(target, artifact, accepted)
    await runQuery()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '复核失败'
  }
}

onMounted(refreshSummary)
</script>

<style scoped>
.muted { color: var(--muted); font-size: 12px; margin-left: 4px; }
.detail-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; }
.plain-list { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 6px; font-size: 13px; }
</style>
