import { externalSearch } from '../config/external-search.js'
import { log } from './log.js'

const providerHandlers = {
	serper: searchSerper,
	brave: searchBrave,
	serpapi: searchSerpapi,
}

const sourceOverrides = {
	'cnn.com': 'CNN',
	'nytimes.com': 'The New York Times',
	'washingtonpost.com': 'The Washington Post',
	'wsj.com': 'The Wall Street Journal',
	'ft.com': 'Financial Times',
	'bbc.com': 'BBC',
	'bbc.co.uk': 'BBC',
	'reuters.com': 'Reuters',
	'bloomberg.com': 'Bloomberg',
	'foxnews.com': 'Fox News',
	'cnbc.com': 'CNBC',
	'politico.com': 'Politico',
	'thehill.com': 'The Hill',
	'axios.com': 'Axios',
	'npr.org': 'NPR',
	'apnews.com': 'AP News',
	'valawyersweekly.com': 'Virginia Lawyers Weekly',
	'milawyersweekly.com': 'Michigan Lawyers Weekly',
	'tradingview.com': 'TradingView',
	'thedailyrecord.com': 'The Daily Record',
}

export function sourceFromUrl(url) {
	if (!url) return ''
	try {
		let host = new URL(url).hostname.replace(/^www\./, '')
		for (let [domain, name] of Object.entries(sourceOverrides)) {
			if (host === domain || host.endsWith(`.${domain}`)) return name
		}
		let parts = host.split('.')
		let base = parts.length >= 2 ? parts[parts.length - 2] : host
		return base.replace(/[-_]+/g, ' ').replace(/\b\w/g, char => char.toUpperCase())
	} catch {
		return ''
	}
}

function normalizeResult(item) {
	if (!item || !item.url) return null
	let url = item.url
	let gnUrl = ''
	try {
		let host = new URL(url).hostname.replace(/^www\./, '')
		if (host === 'news.google.com') {
			gnUrl = url
			url = ''
		}
	} catch {}
	let source = item.source || sourceFromUrl(url || gnUrl)
	return {
		titleEn: item.title || '',
		url,
		gnUrl,
		source: source || '',
	}
}

async function fetchJson(url, { method = 'GET', headers, body, timeoutMs }) {
	let response = await fetch(url, {
		method,
		headers,
		body,
		signal: AbortSignal.timeout(timeoutMs || 10000),
	})
	if (!response.ok) {
		throw new Error(`${response.status} ${response.statusText}`)
	}
	return await response.json()
}

async function searchSerper(query, { apiKey, maxResults, timeoutMs }) {
	let json = await fetchJson('https://google.serper.dev/search', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'X-API-KEY': apiKey,
		},
		body: JSON.stringify({ q: query, num: maxResults }),
		timeoutMs,
	})
	let results = Array.isArray(json?.organic) ? json.organic : []
	return results.map(item => normalizeResult({
		title: item.title,
		url: item.link,
		source: item.source,
	})).filter(Boolean)
}

async function searchBrave(query, { apiKey, maxResults, timeoutMs }) {
	let url = `https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(query)}&count=${maxResults}`
	let json = await fetchJson(url, {
		headers: {
			Accept: 'application/json',
			'X-Subscription-Token': apiKey,
		},
		timeoutMs,
	})
	let results = Array.isArray(json?.web?.results) ? json.web.results : []
	return results.map(item => normalizeResult({
		title: item.title,
		url: item.url,
		source: item.source,
	})).filter(Boolean)
}

async function searchSerpapi(query, { apiKey, maxResults, timeoutMs }) {
	let url = `https://serpapi.com/search.json?q=${encodeURIComponent(query)}&engine=google&num=${maxResults}&api_key=${encodeURIComponent(apiKey)}`
	let json = await fetchJson(url, { timeoutMs })
	let results = Array.isArray(json?.organic_results) ? json.organic_results : []
	return results.map(item => normalizeResult({
		title: item.title,
		url: item.link,
		source: item.source,
	})).filter(Boolean)
}

export async function searchExternal(query) {
	if (!externalSearch?.enabled || !query) return []
	let apiKey = externalSearch.apiKey
	if (!apiKey) return []
	let provider = externalSearch.provider || 'serper'
	let handler = providerHandlers[provider]
	if (!handler) {
		log('external search unsupported provider', provider)
		return []
	}
	try {
		return await handler(query, {
			apiKey,
			maxResults: externalSearch.maxResults || 5,
			timeoutMs: externalSearch.timeoutMs || 10000,
		})
	} catch (error) {
		log('external search failed', provider, error?.message || error)
		return []
	}
}
