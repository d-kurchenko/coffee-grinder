import { log } from './log.js'

const cooldowns = new Map()

function getHost(url) {
	try {
		return new URL(url).hostname.replace(/^www\./, '')
	} catch {
		return ''
	}
}

export function isDomainInCooldown(url) {
	let host = getHost(url)
	if (!host) return null
	let until = cooldowns.get(host)
	if (!until) return null
	if (Date.now() >= until) {
		cooldowns.delete(host)
		return null
	}
	return { host, until, remainingMs: until - Date.now() }
}

export function setDomainCooldown(url, ms, reason) {
	let host = getHost(url)
	if (!host || !ms) return null
	let until = Date.now() + ms
	let existing = cooldowns.get(host) || 0
	if (until > existing) cooldowns.set(host, until)
	log('domain cooldown set', host, Math.ceil(ms / 1000), 's', reason || '')
	return { host, until, reason }
}
