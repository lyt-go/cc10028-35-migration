import { request } from './client'
import type {
  ActionResult,
  IndexSummary,
  ResendEntry,
  ResendResult,
} from '@/data/types'

async function jsonOrThrow<T>(response: Response, fallback: string): Promise<T> {
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = payload && typeof payload.detail === 'string' ? payload.detail : fallback
    throw new Error(detail)
  }
  return payload as T
}

export async function loadIndexSummary(): Promise<IndexSummary> {
  return jsonOrThrow<IndexSummary>(await request('/api/delivery-index'), '交付索引读取失败')
}

export async function migrateLegacy(force = false, resume = true): Promise<Record<string, unknown>> {
  return jsonOrThrow(await request('/api/delivery-index/migrate', {
    method: 'POST',
    body: JSON.stringify({ force, resume }),
  }), '旧交付迁移失败')
}

export async function queryResend(params: { docId?: string; snapshotId?: string }): Promise<ResendResult> {
  const query = new URLSearchParams()
  if (params.docId) query.set('doc_id', params.docId)
  if (params.snapshotId) query.set('snapshot_id', params.snapshotId)
  return jsonOrThrow<ResendResult>(
    await request(`/api/delivery-index/resend?${query.toString()}`),
    '补发查询失败',
  )
}

export async function recordReceipt(snapshotId: string): Promise<ActionResult> {
  return jsonOrThrow<ActionResult>(
    await request(`/api/delivery-index/${encodeURIComponent(snapshotId)}/receipts`, {
      method: 'POST',
      body: JSON.stringify({ receipt_type: '接收回执', received_by: '接收方' }),
    }),
    '回执登记失败',
  )
}

export async function reviewPending(
  snapshotId: string,
  artifact: string,
  accepted: boolean,
): Promise<ActionResult> {
  return jsonOrThrow<ActionResult>(
    await request(`/api/delivery-index/${encodeURIComponent(snapshotId)}/pending-review`, {
      method: 'POST',
      body: JSON.stringify({ artifact, accepted, reviewer: '数据工程组' }),
    }),
    '待核复核失败',
  )
}

export function shardDownloadUrl(snapshotId: string, shardNo: number): string {
  return `/api/delivery-index/${encodeURIComponent(snapshotId)}/shards/${shardNo}`
}

export function downloadShard(entry: ResendEntry, shardNo: number): void {
  const anchor = document.createElement('a')
  anchor.href = shardDownloadUrl(entry.snapshot_id, shardNo)
  anchor.download = `${entry.snapshot_id}_shard_${String(shardNo).padStart(3, '0')}.jsonl`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
}
