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
	let safeParams = sanitizeParams(params)
	if (useStderr) {
		console.error(...safeParams)
		if (duplicate) console.log(...safeParams)
		return
	}
	console.log(...safeParams)
	if (duplicate) console.error(...safeParams)
}
