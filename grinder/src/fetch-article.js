import { log } from './log.js'
import { isDomainInCooldown, setDomainCooldown } from './domain-cooldown.js'
import { fetchConfig } from '../config/fetch.js'

const defaultHeaders = {
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
	'Accept-Language': 'en-US,en;q=0.9',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}
const logAlt = fetchConfig.altFetchLog
const archiveDelayMs = fetchConfig.archiveDelayMs
const archiveCooldownMs = fetchConfig.archiveCooldownMs
let archiveCooldownUntil = 0
let lastArchiveAttempt = 0
const lastFetchStatus = new Map()
const cooldownMsByStatus = {
	401: 10 * 60e3,
	403: 10 * 60e3,
	429: 15 * 60e3,
	500: 2 * 60e3,
	502: 2 * 60e3,
	503: 2 * 60e3,
	504: 2 * 60e3,
}

function getRetryAfterMs(response) {
	let value = response?.headers?.get?.('retry-after')
	if (!value) return 0
	let seconds = Number(value)
	if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000
	let date = Date.parse(value)
	if (!Number.isNaN(date)) {
		let ms = date - Date.now()
		return ms > 0 ? ms : 0
	}
	return 0
}

function buildArchiveUrls(url) {
	let stripped = url.split('?')[0]
	return [
		`https://archive.ph/${stripped}`,
		`https://archive.is/${stripped}`,
		`https://archive.today/${stripped}`,
	]
}

function buildJinaUrl(url) {
	let parsed = new URL(url)
	let protocol = parsed.protocol.replace(':', '')
	return `https://r.jina.ai/${protocol}://${parsed.host}${parsed.pathname}${parsed.search}`
}

function getHost(url) {
	try {
		return new URL(url).hostname.replace(/^www\./, '')
	} catch {
		return ''
	}
}

function setLastStatus(url, status) {
	let host = getHost(url)
	if (host) lastFetchStatus.set(host, status)
}

export function getLastFetchStatus(url) {
	let host = getHost(url)
	if (!host) return null
	return lastFetchStatus.get(host)
}

function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms))
}

async function tryFetchWithStatus(url, label) {
	try {
		let response = await fetch(url, {
			signal: AbortSignal.timeout(10e3),
			headers: defaultHeaders,
		})
		if (!response.ok) {
			if (label) log('article alt fetch failed', label, response.status, response.statusText)
			return { ok: false, status: response.status, statusText: response.statusText }
		}
		let text = await response.text()
		if (label && logAlt) log('article alt fetch ok', label, response.status, text.length)
		return { ok: true, status: response.status, text }
	} catch (e) {
		if (label) log('article alt fetch failed', label, e?.message || e)
		return { ok: false, error: e }
	}
}

async function tryFetch(url, label) {
	try {
		let response = await fetch(url, {
			signal: AbortSignal.timeout(10e3),
			headers: defaultHeaders,
		})
		if (!response.ok) {
			if (label) log('article alt fetch failed', label, response.status, response.statusText)
			return
		}
		let text = await response.text()
		if (label && logAlt) log('article alt fetch ok', label, response.status, text.length)
		return text
	} catch (e) {
		if (label) log('article alt fetch failed', label, e?.message || e)
	}
}

async function tryFetchJson(url, label) {
	try {
		let response = await fetch(url, {
			signal: AbortSignal.timeout(10e3),
			headers: {
				...defaultHeaders,
				Accept: 'application/json,text/plain;q=0.9,*/*;q=0.8',
			},
		})
		if (!response.ok) {
			if (label) log('article alt fetch failed', label, response.status, response.statusText)
			return
		}
		let json = await response.json()
		if (label && logAlt) log('article alt fetch ok', label, response.status, 'json')
		return json
	} catch (e) {
		if (label) log('article alt fetch failed', label, e?.message || e)
	}
}

async function tryWayback(url) {
	let meta = await tryFetchJson(`https://archive.org/wayback/available?url=${encodeURIComponent(url)}`, 'wayback-meta')
	let snapshot = meta?.archived_snapshots?.closest?.url
	if (!snapshot) {
		log('wayback no snapshot')
		return
	}
	let text = await tryFetch(snapshot, 'wayback')
	if (text) return text
	return await tryFetch(buildJinaUrl(snapshot), 'wayback-jina')
}

async function tryArchives(url) {
	if (Date.now() < archiveCooldownUntil) {
		log('archive cooldown active', Math.ceil((archiveCooldownUntil - Date.now()) / 1000), 's')
		return
	}
	for (let archiveUrl of buildArchiveUrls(url)) {
		let wait = archiveDelayMs - (Date.now() - lastArchiveAttempt)
		if (wait > 0) await sleep(wait)
		lastArchiveAttempt = Date.now()
		let result = await tryFetchWithStatus(archiveUrl, `archive:${new URL(archiveUrl).hostname}`)
		if (result?.ok && result.text) return result.text
		if (result?.status === 429) {
			archiveCooldownUntil = Date.now() + archiveCooldownMs
			log('archive cooldown set', Math.ceil(archiveCooldownMs / 1000), 's')
			break
		}
	}
}

export async function fetchArticle(url) {
	let cooldown = isDomainInCooldown(url)
	if (cooldown) {
		log('domain cooldown active', cooldown.host, Math.ceil(cooldown.remainingMs / 1000), 's')
		return
	}
	for (let i = 0; i < 2; i++) {
		try {
			let response = await fetch(url, {
				signal: AbortSignal.timeout(10e3),
				headers: defaultHeaders,
			})
			if (response.ok) {
				setLastStatus(url, 200)
				return await response.text()
			}
			setLastStatus(url, response.status)

			let retryAfter = getRetryAfterMs(response)
			let baseCooldown = cooldownMsByStatus[response.status] || 0
			let cooldownMs = retryAfter || baseCooldown
			if (cooldownMs) setDomainCooldown(url, cooldownMs, response.status)

			if (response.status === 429) {
				log('article fetch failed', response.status, response.statusText)
				return
			}

			if ([401, 403, 429].includes(response.status)) {
				let altText = await tryFetch(buildJinaUrl(url), 'jina')
				if (altText) return altText
				altText = await tryArchives(url)
				if (altText) return altText
				altText = await tryWayback(url)
				if (altText) return altText
			}

			log('article fetch failed', response.status, response.statusText)
		} catch(e) {
			log('article fetch failed', e)
			setLastStatus(url, 'error')
			setDomainCooldown(url, 2 * 60e3, 'error')
		}
	}
	// let response
	// if (paywalled.some(u => url.includes(u))) {
	// 	url = 'https://archive.ph/' + url
	// 	log(url)
	// 	response = await fetch(url)
	// } else {
	// 	response = await fetch(url)
	// 	if (!response.ok) {
	// 		log(response.status, response.statusText)
	// 		url = 'https://archive.ph/' + url
	// 		log(url)
	// 		response = await fetch(url)
	// 	}
	// }
	// if (response.ok) {
	// 	return await response.text()
	// } else {
	// 	log('article fetch failed', response.status, response.statusText)
	// }
}
