import { logging } from '../config/logging.js'

const duplicate = logging.duplicate
const useStderr = logging.useStderr
const maxStringLength = Number.isFinite(logging.maxStringLength)
	? logging.maxStringLength
	: 800

function truncateString(value, limit) {
	if (typeof value !== 'string') return value
	if (!Number.isFinite(limit) || limit <= 0) return value
	if (value.length <= limit) return value
	let suffix = `... (${value.length - limit} more chars)`
	return value.slice(0, Math.max(0, limit - suffix.length)) + suffix
}

function sanitizeParams(params) {
	return params.map(param => truncateString(param, maxStringLength))
}

export function log(...params) {
	if (globalThis.__LOG_SUPPRESS_ALL === true) return
	let safeParams = sanitizeParams(params)
	let suppress = globalThis.__LOG_SUPPRESS === true
	if (suppress) {
		let text = safeParams.map(param => String(param)).join(' ')
		let allowed = text.startsWith('[fatal]')
			|| text.startsWith('[warn] sheet write failed')
			|| text.startsWith('[info] sheet read/write')
		if (!allowed) return
	}
	if (useStderr) {
		console.error(...safeParams)
		if (duplicate) console.log(...safeParams)
		return
	}
	console.log(...safeParams)
	if (duplicate) console.error(...safeParams)
}

export function logTable(...params) {
	if (globalThis.__LOG_SUPPRESS_ALL === true) return
	let safeParams = sanitizeParams(params)
	console.log(...safeParams)
	if (duplicate) console.error(...safeParams)
}
