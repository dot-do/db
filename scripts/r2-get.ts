#!/usr/bin/env npx tsx
import { config } from 'dotenv'
import { resolve } from 'path'
import { createHmac, createHash } from 'crypto'
import { gunzipSync } from 'zlib'

config({ path: resolve(import.meta.dirname, '../.env') })

const r2Url = process.env.R2_URL
const accessKey = process.env.R2_ACCESS_KEY_ID
const secretKey = process.env.R2_SECRET_ACCESS_KEY

if (r2Url === undefined || accessKey === undefined || secretKey === undefined) {
  console.error('Missing R2 creds in .do/db/.env')
  process.exit(1)
}

const key = process.argv[2]
if (key === undefined) {
  console.error('Usage: r2-get.ts <bucket/key>')
  process.exit(1)
}

const region = 'auto'
const service = 's3'
const now = new Date()
const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '')
const amzDate = dateStamp + 'T' + now.toISOString().slice(11, 19).replace(/:/g, '') + 'Z'

const url = new URL(r2Url)
// URI-encode each path segment (S3 requires consistent encoding in canonical request and fetch)
const encodedPath = '/' + key.split('/').map((s) => encodeURIComponent(s)).join('/')
const payloadHash = createHash('sha256').update('').digest('hex')
const canonicalHeaders = `host:${url.host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`
const signedHeaders = 'host;x-amz-content-sha256;x-amz-date'
const canonicalRequest = `GET\n${encodedPath}\n\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`

const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`
const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${createHash('sha256').update(canonicalRequest).digest('hex')}`

function hmac(k: Buffer | string, data: string): Buffer {
  return createHmac('sha256', k).update(data).digest()
}

const signingKey = hmac(hmac(hmac(hmac(`AWS4${secretKey}`, dateStamp), region), service), 'aws4_request')
const signature = createHmac('sha256', signingKey).update(stringToSign).digest('hex')
const authHeader = `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`

const resp = await fetch(`${r2Url}${encodedPath}`, {
  headers: {
    Authorization: authHeader,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
  },
})

if (resp.status !== 200) {
  console.error(`Error ${resp.status}: ${await resp.text()}`)
  process.exit(1)
}

const buf = Buffer.from(await resp.arrayBuffer())
let text: string
try {
  text = gunzipSync(buf).toString('utf-8')
} catch {
  text = buf.toString('utf-8')
}

// Show first N lines
const lines = text.split('\n').filter((l) => l.trim().length > 0)
const limit = Number(process.argv[3]) || 5
console.log(`${lines.length} total lines, showing first ${Math.min(limit, lines.length)}:\n`)
for (let i = 0; i < Math.min(limit, lines.length); i++) {
  try {
    console.log(JSON.stringify(JSON.parse(lines[i]), null, 2))
  } catch {
    console.log(lines[i])
  }
  console.log('---')
}
