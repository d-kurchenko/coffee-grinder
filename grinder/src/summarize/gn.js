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
const gnSearchCache = new Map()
let gnSearchCooldownUntil = 0

function setGnSearchCooldown(ms) {
	gnSearchCooldownUntil = Date.now() + ms
}

function getGnSearchCooldownMs() {
	if (Date.now() < gnSearchCooldownUntil) return gnSearchCooldownUntil - Date.now()
	return 0
}

async function fetchJson(url, timeoutMs) {
	let response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs || 10000) })
	if (!response.ok) {
		throw new Error(`${response.status} ${response.statusText}`)
	}
	return await response.json()
}

const stopwords = new Set([
	'the','a','an','and','or','but','if','then','than','that','this','these','those','to','of','for','in','on','at','by','with','from','as','is','are','was','were','be','been','being','it','its','into','over','after','before','between','about','amid','amidst','against','up','down','out','off','under','again','more','most','some','any','no','not','only','very','just','so','too','also','can','could','may','might','will','would','should','shall','do','does','did','doing','has','have','had','having','i','you','he','she','we','they','them','their','our','your','my',
])

function buildRequiredQuery(text) {
	let cleaned = normalizeTitleForSearch(text)
	if (!cleaned) return ''
	let tokens = cleaned
		.toLowerCase()
		.replace(/[^\p{L}\p{N}\s]+/gu, ' ')
		.split(/\s+/)
		.filter(Boolean)
		.filter(token => token.length > 2 && !stopwords.has(token))
	let uniq = []
	let seen = new Set()
	for (let token of tokens) {
		if (seen.has(token)) continue
		seen.add(token)
		uniq.push(token)
		if (uniq.length >= 6) break
	}
	if (!uniq.length) return `"${cleaned}"`
	return uniq.map(token => `"${token}"`).join(' ')
}

function buildSerpapiQuery(event, fallbackQuery) {
	if (!event) return fallbackQuery || ''
	let title = normalizeTitleForSearch(event._originalTitleEn || event._originalTitleRu || event.titleEn || event.titleRu)
	if (title) return buildRequiredQuery(title)
	let terms = extractSearchTermsFromUrl(event.url || event.gnUrl || '')
	if (terms) return buildRequiredQuery(terms)
	return fallbackQuery || ''
}

function titleRelevant(event, candidate) {
	let target = normalizeTitleKey(event?._originalTitleEn || event?._originalTitleRu || event?.titleEn || event?.titleRu || '')
	if (!target) {
		let terms = extractSearchTermsFromUrl(event?.url || event?.gnUrl || '')
		if (terms) target = normalizeTitleKey(terms)
	}
	if (!target) return false
	let cand = normalizeTitleKey(candidate?.titleEn || extractSearchTermsFromUrl(candidate?.url || ''))
	if (!cand) return false
	if (cand === target) return true
	if (cand.includes(target) || target.includes(cand)) return true
	let targetTokens = new Set(target.split(/\s+/).filter(Boolean))
	let candTokens = new Set(cand.split(/\s+/).filter(Boolean))
	if (targetTokens.size <= 2) return false
	let common = 0
	for (let token of targetTokens) {
		if (candTokens.has(token)) common++
	}
	let ratio = common / Math.max(targetTokens.size, candTokens.size)
	return common >= 2 && ratio >= 0.3
}

async function searchGoogleNewsViaSerpapi(query) {
	if (!externalSearch?.enabled || externalSearch.provider !== 'serpapi' || !externalSearch.apiKey) return []
	if (!query) return []
	let maxResults = externalSearch.maxResults || 6
	let apiKey = encodeURIComponent(externalSearch.apiKey)
	let url = `https://serpapi.com/search.json?engine=google_news&hl=en&gl=us&num=${maxResults}&q=${encodeURIComponent(query)}&api_key=${apiKey}`
	try {
		let json = await fetchJson(url, externalSearch.timeoutMs || 10000)
		let results = Array.isArray(json?.news_results) ? json.news_results : []
		let items = []
		for (let item of results) {
			if (item) items.push(item)
			if (Array.isArray(item?.stories)) {
				for (let story of item.stories) items.push(story)
			}
		}
		let mapped = items.map((item, index) => {
			let rawRank = item?.position ?? item?.rank ?? item?.index
			let parsedRank = Number(rawRank)
			let rank = Number.isFinite(parsedRank) ? parsedRank : (index + 1)
			let link = item?.link || item?.url || ''
			let gnUrl = ''
			let directUrl = link
			if (link) {
				try {
					let host = new URL(link).hostname.replace(/^www\./, '')
					if (host === 'news.google.com') {
						gnUrl = link
						directUrl = ''
					}
				} catch {}
			}
			let source = item?.source?.title || item?.source?.name || item?.source || ''
			if (!source && link) source = sourceFromUrl(link)
			return {
				titleEn: item?.title || '',
				url: directUrl,
				gnUrl,
				source: source || '',
				origin: 'serpapi',
				rank,
			}
		}).filter(item => item.source && item.url)
		return mapped
	} catch (error) {
		log('Google News serpapi fallback failed', error?.message || error)
		return []
	}
}

function formatSerpapiSample(list, limit = 3) {
	if (!Array.isArray(list) || !list.length) return ''
	return list.slice(0, limit).map(item => {
		let title = (item.titleEn || '').replace(/\s+/g, ' ').trim()
		let url = item.url || item.gnUrl || ''
		let source = item.source || ''
		return `${source}: ${title} (${url})`
	}).join(' | ')
}

function summarizeResultsForLog(results, limit = 8) {
	if (!Array.isArray(results) || !results.length) return []
	return results.slice(0, limit).map(item => ({
		source: item.source || '',
		title: (item.titleEn || item.titleRu || '').replace(/\s+/g, ' ').trim(),
		url: item.url || '',
		gnUrl: item.gnUrl || '',
		origin: item.origin || item.provider || item.from || '',
		rank: Number.isFinite(Number(item.rank)) ? Number(item.rank) : undefined,
	}))
}

export function parseRelatedArticles(description) {
	if (!description) return []
	try {
		let json = xml2json(description, { compact: true })
		let list = JSON.parse(json)?.ol?.li
		if (!list) return []
		if (!Array.isArray(list)) list = [list]
		return list.map(({ a, font }, index) => ({
			titleEn: a?._text || '',
			gnUrl: a?._attributes?.href || '',
			source: font?._text || '',
			origin: 'gn',
			rank: index + 1,
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
		return items.map((event, index) => {
			let articles = parseRelatedArticles(event.description?._text)
			return {
				titleEn: event.title?._text || '',
				gnUrl: event.link?._text || '',
				source: event.source?._text || '',
				origin: 'gn',
				date: event.pubDate?._text ? new Date(event.pubDate._text) : null,
				articles,
				rank: index + 1,
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
	let title = normalizeTitleForSearch(event._originalTitleEn || event._originalTitleRu || event.titleEn || event.titleRu || event.title || '')
	let queries = []
	const extractTerms = url => {
		if (!url) return ''
		let terms = extractSearchTermsFromUrl(url)
		if (terms) return terms
		if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(url)) {
			return extractSearchTermsFromUrl(`https://${url}`)
		}
		return ''
	}
	if (title) {
		let q = buildRequiredQuery(title)
		if (q) queries.push(q)
	}
	if (!queries.length) {
		let url = event._originalUrl || event.url || event.alternativeUrl || event.gnUrl || ''
		let terms = extractTerms(url)
		if (terms) {
			let q = buildRequiredQuery(terms)
			if (q) queries.push(q)
		}
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

export async function searchGoogleNews(query, last, event) {
	if (!query) return []
	let cooldownMs = getGnSearchCooldownMs()
	if (cooldownMs > 0) {
		log('Google News search cooldown active', Math.ceil(cooldownMs / 1000), 's')
		let serpapiQuery = buildSerpapiQuery(event, query)
		log('SerpAPI query', serpapiQuery)
		let serpapiResults = await searchGoogleNewsViaSerpapi(serpapiQuery)
		if (event) {
			let before = serpapiResults
			let after = serpapiResults.filter(item => titleRelevant(event, item))
			log('SerpAPI results', before.length, '| accepted', after.length, '| rejected', before.length - after.length)
			log('SerpAPI accepted sample', formatSerpapiSample(after))
			if (before.length && before.length - after.length) {
				let rejected = before.filter(item => !after.includes(item))
				log('SerpAPI rejected sample', formatSerpapiSample(rejected))
			}
			serpapiResults = after
		}
		if (serpapiResults.length) {
			log('Google News search fallback (serpapi) returned', serpapiResults.length)
			gnSearchCache.set(query, { time: Date.now(), results: serpapiResults })
		}
		return serpapiResults
	}
	let cache = gnSearchCache.get(query)
	if (cache && (Date.now() - cache.time) < 5 * 60e3) {
		return cache.results
	}
	log('Google News query', query)
	await sleep(last.gnSearch.time + last.gnSearch.delay - Date.now())
	last.gnSearch.time = Date.now()
	let url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&${googleNewsDefaults}`
	try {
		let response = await fetch(url, {
			headers: {
				'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
				'Accept-Language': 'en-US,en;q=0.9',
				'Accept': 'application/rss+xml,application/xml;q=0.9,*/*;q=0.8',
			},
		})
		if (!response.ok) {
			log('Google News search failed', response.status, response.statusText)
			if (response.status === 503 || response.status === 429) {
				setGnSearchCooldown(10 * 60e3)
			}
			let serpapiQuery = buildSerpapiQuery(event, query)
			log('SerpAPI query', serpapiQuery)
			let serpapiResults = await searchGoogleNewsViaSerpapi(serpapiQuery)
			if (event) {
				let before = serpapiResults
				let after = serpapiResults.filter(item => titleRelevant(event, item))
				log('SerpAPI results', before.length, '| accepted', after.length, '| rejected', before.length - after.length)
				log('SerpAPI accepted sample', formatSerpapiSample(after))
				if (before.length && before.length - after.length) {
					let rejected = before.filter(item => !after.includes(item))
					log('SerpAPI rejected sample', formatSerpapiSample(rejected))
				}
				serpapiResults = after
			}
			if (serpapiResults.length) {
				log('Google News search fallback (serpapi) returned', serpapiResults.length)
				gnSearchCache.set(query, { time: Date.now(), results: serpapiResults })
			}
			return serpapiResults
		}
		let xml = await response.text()
		let results = parseGoogleNewsXml(xml)
		log('Google News results', results.length)
		if (results.length) log('Google News sample', formatSerpapiSample(results))
		gnSearchCache.set(query, { time: Date.now(), results })
		return results
	} catch(e) {
		log('Google News search failed', e)
		let serpapiQuery = buildSerpapiQuery(event, query)
		log('SerpAPI query', serpapiQuery)
		let serpapiResults = await searchGoogleNewsViaSerpapi(serpapiQuery)
		if (event) {
			let before = serpapiResults
			let after = serpapiResults.filter(item => titleRelevant(event, item))
			log('SerpAPI results', before.length, '| accepted', after.length, '| rejected', before.length - after.length)
			log('SerpAPI accepted sample', formatSerpapiSample(after))
			if (before.length && before.length - after.length) {
				let rejected = before.filter(item => !after.includes(item))
				log('SerpAPI rejected sample', formatSerpapiSample(rejected))
			}
			serpapiResults = after
		}
		if (serpapiResults.length) {
			log('Google News search fallback (serpapi) returned', serpapiResults.length)
			gnSearchCache.set(query, { time: Date.now(), results: serpapiResults })
		}
		return serpapiResults
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
	let bestRank = Number.POSITIVE_INFINITY
	let usedQuery = ''
	for (let query of uniqueQueries) {
		let results = await searchGoogleNews(query, last, event)
		if (logEvent) {
			logEvent(event, {
				phase: 'gn_backfill_search',
				status: results.length ? 'ok' : 'empty',
				query,
				count: results.length,
				results: summarizeResultsForLog(results),
			}, `#${event.id} GN backfill search ${results.length}`, results.length ? 'info' : 'warn')
		}
		if (!results.length) continue
		usedQuery = query
		for (let item of results.slice(0, 6)) {
			let score = scoreGnCandidate(event, item)
			let rank = Number.isFinite(Number(item?.rank)) ? Number(item.rank) : Number.POSITIVE_INFINITY
			if (score > bestScore || (score === bestScore && rank < bestRank)) {
				best = item
				bestScore = score
				bestRank = rank
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
	let results = await searchGoogleNews(query, last, event)
	if (logEvent) {
		logEvent(event, {
			phase: 'gn_search',
			status: results.length ? 'ok' : 'empty',
			query,
			count: results.length,
			results: summarizeResultsForLog(results),
		}, `#${event.id} GN search ${results.length}`, results.length ? 'info' : 'warn')
	}
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
