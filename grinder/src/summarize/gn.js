import { xml2json } from 'xml-js'

import { log } from '../log.js'
import { sleep } from '../sleep.js'
import { externalSearch } from '../../config/external-search.js'
import { searchExternal, sourceFromUrl } from '../external-search.js'
import { getArticles, setArticles } from './articles.js'
import {
	extractSearchTermsFromUrl,
	isBlank,
	normalizeSource,
	normalizeTitleForSearch,
	normalizeTitleKey,
} from './utils.js'

const googleNewsDefaults = 'hl=en-US&gl=US&ceid=US:en'

export function parseRelatedArticles(description) {
	if (!description) return []
	try {
		let json = xml2json(description, { compact: true })
		let list = JSON.parse(json)?.ol?.li
		if (!list) return []
		if (!Array.isArray(list)) list = [list]
		return list.map(({ a, font }) => ({
			titleEn: a?._text || '',
			gnUrl: a?._attributes?.href || '',
			source: font?._text || '',
		})).filter(article => article.gnUrl && article.source)
	} catch {
		return []
	}
}

export function parseGoogleNewsXml(xml) {
	try {
		let feed = JSON.parse(xml2json(xml, { compact: true }))
		let items = feed?.rss?.channel?.item
		if (!items) return []
		if (!Array.isArray(items)) items = [items]
		return items.map(event => {
			let articles = parseRelatedArticles(event.description?._text)
			return {
				titleEn: event.title?._text || '',
				gnUrl: event.link?._text || '',
				source: event.source?._text || '',
				date: event.pubDate?._text ? new Date(event.pubDate._text) : null,
				articles,
			}
		}).filter(item => item.gnUrl)
	} catch {
		return []
	}
}

export function buildSearchQuery(event) {
	let title = normalizeTitleForSearch(event._originalTitleEn || event._originalTitleRu || event.titleEn || event.titleRu)
	if (!isBlank(title)) return `"${title}"`
	if (!isBlank(event.url)) {
		try {
			let parsed = new URL(event.url)
			let slug = parsed.pathname.split('/').filter(Boolean).pop() || ''
			let terms = slug.replace(/[-_]/g, ' ').trim()
			let host = parsed.hostname.replace(/^www\./, '')
			return terms ? `site:${host} ${terms}` : `site:${host}`
		} catch {
			return event.url
		}
	}
	return ''
}

export function buildFallbackSearchQueries(event) {
	let title = normalizeTitleForSearch(event._originalTitleEn || event._originalTitleRu || event.titleEn || event.titleRu)
	let queries = []
	if (title) {
		queries.push(`"${title}"`)
		queries.push(title)
		let short = title.split(/\s+/).slice(0, 10).join(' ')
		if (short && short !== title) queries.push(short)
	}
	if (!queries.length && !isBlank(event.url)) {
		let terms = extractSearchTermsFromUrl(event.url)
		if (terms) queries.push(terms)
		else queries.push(event.url)
	}
	let seen = new Set()
	let unique = []
	for (let q of queries) {
		if (!q) continue
		let key = q.toLowerCase()
		if (seen.has(key)) continue
		seen.add(key)
		unique.push(q)
	}
	return unique.slice(0, 3)
}

export function scoreGnCandidate(event, candidate) {
	let targetTitle = normalizeTitleKey(event.titleEn || event.titleRu || '')
	let targetSource = normalizeSource(event.source) || normalizeSource(sourceFromUrl(event.url))
	let candTitle = normalizeTitleKey(candidate.titleEn || '')
	let candSource = normalizeSource(candidate.source || '')
	let score = 0
	if (targetTitle && candTitle) {
		if (targetTitle === candTitle) score += 3
		else if (candTitle.includes(targetTitle) || targetTitle.includes(candTitle)) score += 1
	}
	if (targetSource && candSource && targetSource === candSource) score += 2
	return score
}

export async function searchGoogleNews(query, last) {
	if (!query) return []
	await sleep(last.gnSearch.time + last.gnSearch.delay - Date.now())
	last.gnSearch.time = Date.now()
	let url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&${googleNewsDefaults}`
	try {
		let response = await fetch(url)
		if (!response.ok) {
			log('Google News search failed', response.status, response.statusText)
			return []
		}
		let xml = await response.text()
		return parseGoogleNewsXml(xml)
	} catch(e) {
		log('Google News search failed', e)
		return []
	}
}

export async function backfillGnUrl(event, last, { logEvent } = {}) {
	if (!isBlank(event.gnUrl)) return false
	let queries = buildFallbackSearchQueries(event)
	let shortTitle = normalizeTitleForSearch(event.titleEn || event.titleRu || '')
	if (shortTitle && event.source) queries.unshift(`"${shortTitle}" ${event.source}`)
	if (shortTitle && event.url) {
		try {
			let host = new URL(event.url).hostname.replace(/^www\./, '')
			queries.unshift(`site:${host} ${shortTitle}`)
			if (event.url.includes('reuters.com')) queries.unshift(`site:reuters.com ${shortTitle}`)
		} catch {}
	}
	let seen = new Set()
	let uniqueQueries = []
	for (let query of queries) {
		let key = query.toLowerCase()
		if (seen.has(key)) continue
		seen.add(key)
		uniqueQueries.push(query)
	}
	if (!uniqueQueries.length) return false
	let best = null
	let bestScore = -1
	let usedQuery = ''
	for (let query of uniqueQueries) {
		let results = await searchGoogleNews(query, last)
		if (!results.length) continue
		usedQuery = query
		for (let item of results.slice(0, 6)) {
			let score = scoreGnCandidate(event, item)
			if (score > bestScore) {
				best = item
				bestScore = score
			}
		}
		if (bestScore >= 3) break
	}
	if (!best) {
		if (externalSearch?.enabled && externalSearch.apiKey) {
			let extQueries = []
			if (shortTitle) {
				extQueries.push(`site:news.google.com "${shortTitle}"`)
				if (event.source) extQueries.push(`site:news.google.com "${shortTitle}" ${event.source}`)
			}
			let termsFromUrl = extractSearchTermsFromUrl(event.url)
			if (termsFromUrl) extQueries.push(`site:news.google.com ${termsFromUrl}`)
			for (let query of extQueries.slice(0, 3)) {
				let results = await searchExternal(query)
				let gnResult = results.find(item => item.gnUrl)
				if (gnResult?.gnUrl) {
					event.gnUrl = gnResult.gnUrl
					if (isBlank(event.titleEn) && gnResult.titleEn) event.titleEn = gnResult.titleEn
					if (isBlank(event.source) && gnResult.source) event.source = gnResult.source
					if (logEvent) {
						logEvent(event, {
							phase: 'gn_backfill_external',
							status: 'ok',
							query,
							source: gnResult.source,
						}, '', 'info')
					}
					return true
				}
			}
		}
		if (logEvent) {
			logEvent(event, {
				phase: 'gn_backfill',
				status: 'empty',
				queries: uniqueQueries,
			}, `#${event.id} google news link not found`, 'warn')
		}
		return false
	}
	let changed = false
	if (isBlank(event.titleEn) && best.titleEn) {
		event.titleEn = best.titleEn
		changed = true
	}
	if (isBlank(event.source) && best.source) {
		event.source = best.source
		changed = true
	}
	if (isBlank(event.gnUrl) && best.gnUrl) {
		event.gnUrl = best.gnUrl
		changed = true
	}
	if (changed && logEvent) {
		logEvent(event, {
			phase: 'gn_backfill',
			status: 'ok',
			query: usedQuery || uniqueQueries[0],
			source: best.source,
		}, '', 'info')
	}
	return changed
}

export async function hydrateFromGoogleNews(event, last, { decodeUrl, logEvent } = {}) {
	let hasMeta = !isBlank(event.titleEn) && !isBlank(event.source) && !isBlank(event.gnUrl)
	let hasArticles = getArticles(event).length > 0
	if (hasMeta && hasArticles) return false

	let query = buildSearchQuery(event)
	let results = await searchGoogleNews(query, last)
	if (!results.length) return false

	let best = results[0]
	if (isBlank(event.titleEn) && best.titleEn) event.titleEn = best.titleEn
	if (isBlank(event.source) && best.source) event.source = best.source
	if (isBlank(event.gnUrl) && best.gnUrl) event.gnUrl = best.gnUrl

	if (!hasArticles) {
		let articles = best.articles?.length
			? best.articles
			: results.map(item => ({
				titleEn: item.titleEn || '',
				gnUrl: item.gnUrl || '',
				source: item.source || '',
			})).filter(item => item.gnUrl && item.source)
		if (articles.length) {
			setArticles(event, articles)
		}
	}

	if (isBlank(event.url) && !isBlank(event.gnUrl) && typeof decodeUrl === 'function') {
		event.url = await decodeUrl(event.gnUrl, last)
	}

	if (logEvent) {
		logEvent(event, {
			phase: 'gn_search',
			status: 'ok',
			query,
		}, `#${event.id} google news metadata filled`, 'info')
	}
	return true
}
