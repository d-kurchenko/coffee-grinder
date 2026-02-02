import fs from 'fs'
import path from 'path'

import { log } from './log.js'
import { logging } from '../config/logging.js'

const maxDataStringLength = Number.isFinite(logging.maxDataStringLength)
	? logging.maxDataStringLength
	: 800
const maxLogBytes = Number.isFinite(logging.fetchLogMaxBytes)
	? logging.fetchLogMaxBytes
	: 0
const maxLogFiles = Number.isFinite(logging.fetchLogMaxFiles)
	? logging.fetchLogMaxFiles
	: 0

function truncateString(value, limit) {
	if (typeof value !== 'string') return value
	if (!Number.isFinite(limit) || limit <= 0) return value
	if (value.length <= limit) return value
	let suffix = `... (${value.length - limit} more chars)`
	return value.slice(0, Math.max(0, limit - suffix.length)) + suffix
}

function sanitizeData(value, limit) {
	if (typeof value === 'string') return truncateString(value, limit)
	if (Array.isArray(value)) return value.map(item => sanitizeData(item, limit))
	if (value && typeof value === 'object') {
		let out = {}
		for (let [key, val] of Object.entries(value)) {
			out[key] = sanitizeData(val, limit)
		}
		return out
	}
	return value
}

function rotateLogIfNeeded(logFile) {
	if (!maxLogBytes || maxLogBytes <= 0) return
	if (!fs.existsSync(logFile)) return
	try {
		let size = fs.statSync(logFile).size
		if (size < maxLogBytes) return
		let keep = Math.max(0, Math.floor(maxLogFiles || 0))
		if (keep <= 0) {
			fs.truncateSync(logFile, 0)
			return
		}
		let oldest = `${logFile}.${keep}`
		if (fs.existsSync(oldest)) fs.rmSync(oldest)
		for (let i = keep - 1; i >= 1; i--) {
			let src = `${logFile}.${i}`
			if (fs.existsSync(src)) fs.renameSync(src, `${logFile}.${i + 1}`)
		}
		fs.renameSync(logFile, `${logFile}.1`)
	} catch (error) {
		log('[warn]', `fetch log rotate failed: ${error?.message || error}`)
	}
}

export function logFetch(data, message, level = 'info') {
	let prefix = level ? `[${level}]` : ''
	if (message) log(prefix, message)
	if (data) {
		let safeData = sanitizeData(data, maxDataStringLength)
		let logFile = logging.fetchLogFile
		if (logFile) {
			let line = JSON.stringify({
				ts: new Date().toISOString(),
				level,
				message,
				...safeData,
			})
			try {
				fs.mkdirSync(path.dirname(logFile), { recursive: true })
				rotateLogIfNeeded(logFile)
				fs.appendFileSync(logFile, line + '\n')
			} catch (error) {
				log('[warn]', `fetch log write failed: ${error?.message || error}`)
			}
		}
		if (logging.fetchJson) {
			log(prefix, JSON.stringify(safeData))
		}
	}
}
