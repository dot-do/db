#!/usr/bin/env npx tsx
/**
 * Empty an R2 bucket by listing and batch-deleting all objects.
 * Uses S3 DeleteObjects API (1000 per request).
 */
import { config } from 'dotenv'
import { resolve } from 'path'
import { createHmac, createHash } from 'crypto'

config({ path: resolve(import.meta.dirname, '../.env') })

const r2Url = process.env.R2_URL
const accessKey = process.env.R2_ACCESS_KEY_ID
const secretKey = process.env.R2_SECRET_ACCESS_KEY

if (!r2Url || !accessKey || !secretKey) {
  console.error('Missing R2 creds in .do/db/.env')
  process.exit(1)
}

const bucket = process.argv[2]
if (!bucket) {
  console.error('Usage: r2-empty.ts <bucket>')
  process.exit(1)
}

const region = 'auto'
const service = 's3'
const url = new URL(r2Url)

function hmac(k: Buffer | string, data: string): Buffer {
  return createHmac('sha256', k).update(data).digest()
}

function sign(method: string, path: string, qs: string, body: string, contentType?: string): Record<string, string> {
  const now = new Date()
  const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '')
  const amzDate = dateStamp + 'T' + now.toISOString().slice(11, 19).replace(/:/g, '') + 'Z'
  const payloadHash = createHash('sha256').update(body).digest('hex')

  let canonicalHeaders = `host:${url.host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`
  let signedHeaders = 'host;x-amz-content-sha256;x-amz-date'

  if (contentType) {
    canonicalHeaders = `content-type:${contentType}\n${canonicalHeaders}`
    signedHeaders = `content-type;${signedHeaders}`
  }

  const canonicalRequest = `${method}\n${path}\n${qs}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${createHash('sha256').update(canonicalRequest).digest('hex')}`
  const signingKey = hmac(hmac(hmac(hmac(`AWS4${secretKey}`, dateStamp), region), service), 'aws4_request')
  const signature = createHmac('sha256', signingKey).update(stringToSign).digest('hex')

  const headers: Record<string, string> = {
    Authorization: `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
  }
  if (contentType) headers['Content-Type'] = contentType
  return headers
}

async function listObjects(continuationToken?: string): Promise<{ keys: string[]; nextToken?: string }> {
  const path = `/${bucket}`

  // Build query params as sorted key=value pairs (S3 SigV4 requires sorted canonical QS)
  const params: [string, string][] = [
    ['list-type', '2'],
    ['max-keys', '1000'],
  ]
  if (continuationToken) params.push(['continuation-token', continuationToken])
  params.sort((a, b) => a[0].localeCompare(b[0]))

  // For canonical request: encode key and value separately
  const canonicalQs = params.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&')

  const headers = sign('GET', path, canonicalQs, '')
  const resp = await fetch(`${r2Url}${path}?${canonicalQs}`, { headers })
  if (!resp.ok) throw new Error(`List failed: ${resp.status} ${await resp.text()}`)

  const text = await resp.text()
  const keys = [...text.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1])
  const isTruncated = text.includes('<IsTruncated>true</IsTruncated>')
  const nextMatch = text.match(/<NextContinuationToken>([^<]+)<\/NextContinuationToken>/)

  return { keys, nextToken: isTruncated && nextMatch ? nextMatch[1] : undefined }
}

async function deleteObjects(keys: string[]): Promise<void> {
  const path = `/${bucket}`
  const qs = 'delete='
  const body = `<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>true</Quiet>${keys.map((k) => `<Object><Key>${k}</Key></Object>`).join('')}</Delete>`
  const contentMd5 = createHash('md5').update(body).digest('base64')

  const headers = sign('POST', path, qs, body, 'application/xml')
  headers['Content-MD5'] = contentMd5

  const resp = await fetch(`${r2Url}${path}?${qs}`, { method: 'POST', headers, body })
  if (!resp.ok) throw new Error(`Delete failed: ${resp.status} ${await resp.text()}`)
}

let total = 0
let token: string | undefined
do {
  const { keys, nextToken } = await listObjects(token)
  if (keys.length === 0) break

  await deleteObjects(keys)
  total += keys.length
  process.stdout.write(`\rDeleted ${total.toLocaleString()} objects...`)
  token = nextToken
} while (token)

console.log(`\nDone. Deleted ${total.toLocaleString()} objects from bucket '${bucket}'.`)
