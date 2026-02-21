#!/usr/bin/env npx tsx
import { config } from 'dotenv'
import { resolve } from 'path'
import { createHmac, createHash } from 'crypto'

config({ path: resolve(import.meta.dirname, '../.env') })

const r2Url = process.env.R2_URL
const accessKey = process.env.R2_ACCESS_KEY_ID
const secretKey = process.env.R2_SECRET_ACCESS_KEY

if (r2Url === undefined || accessKey === undefined || secretKey === undefined) {
  console.error('Missing R2_URL, R2_ACCESS_KEY_ID, or R2_SECRET_ACCESS_KEY in .do/db/.env')
  process.exit(1)
}

const bucket = process.argv[2] || 'events'
const prefix = process.argv[3] || ''
const maxKeys = process.argv[4] || '20'

// AWS Signature V4 for S3-compatible API
const region = 'auto'
const service = 's3'
const now = new Date()
const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '')
const amzDate = dateStamp + 'T' + now.toISOString().slice(11, 19).replace(/:/g, '') + 'Z'

const url = new URL(r2Url)
const path = `/${bucket}`
const qs = `delimiter=%2F&list-type=2&max-keys=${maxKeys}${prefix ? `&prefix=${encodeURIComponent(prefix)}` : ''}`

const payloadHash = createHash('sha256').update('').digest('hex')
const canonicalHeaders = `host:${url.host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`
const signedHeaders = 'host;x-amz-content-sha256;x-amz-date'
const canonicalRequest = `GET\n${path}\n${qs}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`

const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`
const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${createHash('sha256').update(canonicalRequest).digest('hex')}`

function hmac(key: Buffer | string, data: string): Buffer {
  return createHmac('sha256', key).update(data).digest()
}

const signingKey = hmac(hmac(hmac(hmac(`AWS4${secretKey}`, dateStamp), region), service), 'aws4_request')
const signature = createHmac('sha256', signingKey).update(stringToSign).digest('hex')
const authHeader = `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`

const resp = await fetch(`${r2Url}${path}?${qs}`, {
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

const text = await resp.text()

const prefixes = [...text.matchAll(/<CommonPrefixes><Prefix>([^<]+)<\/Prefix><\/CommonPrefixes>/g)].map((m) => m[1])
const keys = [...text.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1])
const sizes = [...text.matchAll(/<Size>([^<]+)<\/Size>/g)].map((m) => Number(m[1]))

if (prefixes.length > 0) {
  console.log('Prefixes:')
  for (const p of prefixes) console.log(`  ${p}`)
}

if (keys.length > 0) {
  console.log('Objects:')
  for (let i = 0; i < keys.length; i++) {
    const size = sizes[i] !== undefined ? ` (${(sizes[i] / 1024).toFixed(1)} KB)` : ''
    console.log(`  ${keys[i]}${size}`)
  }
}

if (prefixes.length === 0 && keys.length === 0) {
  console.log('Empty (no objects or prefixes)')
}
