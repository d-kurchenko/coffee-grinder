import { minAgencyLevel, alternativeDateWindowDays } from '../../config/verification.js'
import { extractSearchTermsFromUrl, getAgencyLevel, getArticleLink, normalizeSource, normalizeTitleKey } from './utils.js'

const runtimeArticlesKey = `run_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`

function parseDate(value) {
	if (!value) return null
	if (value instanceof Date) return Number.isFinite(value.getTime()) ? value : null
	let parsed = new Date(value)
	return Number.isFinite(parsed.getTime()) ? parsed : null
}

function getEventDate(event) {
	return parseDate(event?._originalDate || event?.date)
}

function isWithinDateWindow(eventDate, candidateDate) {
	if (!Number.isFinite(alternativeDateWindowDays) || alternativeDateWindowDays <= 0) return true
	if (!eventDate || !candidateDate) return true
	let diffDays = Math.abs(candidateDate.getTime() - eventDate.getTime()) / 864e5
	return diffDays <= alternativeDateWindowDays
}

export function isRuntimeArticles(event) {
	return event?._articlesOrigin === runtimeArticlesKey
}

function titleMatches(targetKey, candidateKey) {
	if (!targetKey) return false
	if (!candidateKey) return false
	let targetTokens = new Set(targetKey.split(/\s+/).filter(Boolean))
	let candidateTokens = new Set(candidateKey.split(/\s+/).filter(Boolean))
	if (!targetTokens.size || !candidateTokens.size) return false
	let common = 0
	for (let token of targetTokens) {
		if (candidateTokens.has(token)) common++
	}
	if (targetTokens.size <= 2) return common === targetTokens.size
	let ratio = common / Math.max(targetTokens.size, candidateTokens.size)
	return common >= 2 && ratio >= 0.3
}

function getTargetTitleKey(event) {
	let title = event?.titleEn || event?.titleRu || event?._originalTitleEn || event?._originalTitleRu || ''
	let key = normalizeTitleKey(title)
	if (key) return key
	let link = getArticleLink(event) || event?.url || event?.gnUrl || ''
	let terms = extractSearchTermsFromUrl(link)
	if (!terms) return ''
	return normalizeTitleKey(terms)
}

function getCandidateTitleKey(item) {
	let link = getArticleLink(item) || item?.url || item?.gnUrl || ''
	let terms = extractSearchTermsFromUrl(link)
	let slugKey = terms ? normalizeTitleKey(terms) : ''
	let title = item?.titleEn || item?.titleRu || ''
	let titleKey = normalizeTitleKey(title)
	// For sheet-originated entries, trust the URL slug over stored title (can be stale/mismatched).
	if (item?.origin === 'sheet') return slugKey || titleKey || ''
	return titleKey || slugKey || ''
}

export function parseArticlesValue(value) {
	if (!value) return []
	if (Array.isArray(value)) return value
	if (typeof value !== 'string') return []
	try {
		let parsed = JSON.parse(value)
		return Array.isArray(parsed) ? parsed : []
	} catch {
		return []
	}
}

export function setArticles(event, articles) {
	event._articles = articles
	event._articlesOrigin = runtimeArticlesKey
}

export function getArticles(event) {
	if (!isRuntimeArticles(event)) return []
	if (Array.isArray(event?._articles)) return event._articles
	return []
}

function buildAlternativeArticles(event, candidates) {
	let currentSource = normalizeSource(event.source)
	let currentLink = getArticleLink(event)
	let seen = new Set([currentSource])
	let seenTitle = new Set()
	let targetDate = getEventDate(event)
	let items = (candidates || [])
		.filter(article => getArticleLink(article) && article?.source && article?.origin !== 'sheet')
		.map(article => ({
			...article,
			url: article?.url || '',
			gnUrl: article?.gnUrl || '',
			level: getAgencyLevel(article.source),
			normalizedSource: normalizeSource(article.source),
			hasDirectUrl: Boolean(article?.url),
			normalizedTitle: getCandidateTitleKey(article),
			origin: article?.origin || article?.provider || article?.from || (article?.gnUrl ? 'gn' : ''),
			parsedDate: parseDate(article?.date),
			rank: Number.isFinite(Number(article?.rank))
				? Number(article.rank)
				: (Number.isFinite(Number(article?.position)) ? Number(article.position) : null),
		}))
	let filtered = []
	for (let article of items) {
		if (!article.normalizedSource) continue
		if (!isWithinDateWindow(targetDate, article.parsedDate)) continue
		if (Number.isFinite(minAgencyLevel) && article.level < minAgencyLevel) continue
		if (seen.has(article.normalizedSource)) {
			let link = getArticleLink(article)
			if (!currentLink || !link || link === currentLink) continue
		}
		let titleKey = article.normalizedTitle
			? `${article.normalizedSource}|${article.normalizedTitle}`
			: `${article.normalizedSource}|__no_title__`
		if (seenTitle.has(titleKey)) continue
		seenTitle.add(titleKey)
		seen.add(article.normalizedSource)
		filtered.push(article)
	}
	return filtered.sort((a, b) => {
		let levelDiff = (b.level - a.level)
		if (levelDiff) return levelDiff
		let directDiff = (b.hasDirectUrl ? 1 : 0) - (a.hasDirectUrl ? 1 : 0)
		if (directDiff) return directDiff
		let rankA = Number.isFinite(Number(a.rank)) ? Number(a.rank) : Number.POSITIVE_INFINITY
		let rankB = Number.isFinite(Number(b.rank)) ? Number(b.rank) : Number.POSITIVE_INFINITY
		return rankA - rankB
	})
}

export function getAlternativeArticles(event) {
	if (!isRuntimeArticles(event)) return []
	return buildAlternativeArticles(event, getArticles(event))
}

export function classifyAlternativeCandidates(event, candidates) {
	let items = Array.isArray(candidates) ? candidates : getArticles(event)
	let accepted = buildAlternativeArticles(event, items)
	let acceptedKeys = new Set(accepted.map(item => {
		let link = getArticleLink(item)
		let sourceKey = normalizeSource(item.source)
		return `${sourceKey}|${link}`
	}))
	let currentSource = normalizeSource(event.source)
	let currentLink = getArticleLink(event)
	let targetDate = getEventDate(event)
	let rejected = []
	for (let article of items) {
		let link = getArticleLink(article)
		let sourceKey = normalizeSource(article.source)
		let key = `${sourceKey}|${link}`
		let reason = ''
		let candidateDate = parseDate(article?.date)
		let level = getAgencyLevel(article.source)
		if (!link || !article?.source) {
			reason = 'missing_link_or_source'
		} else if (article.origin === 'sheet') {
			reason = 'sheet_origin'
		} else if (!isWithinDateWindow(targetDate, candidateDate)) {
			reason = 'date_out_of_range'
		} else if (sourceKey && sourceKey === currentSource && link === currentLink) {
			reason = 'same_source_same_link'
		} else if (acceptedKeys.has(key)) {
			continue
		} else if (Number.isFinite(minAgencyLevel) && level < minAgencyLevel) {
			reason = 'below_min_agency'
		} else {
			reason = 'filtered'
		}
		rejected.push({
			...article,
			reason,
			level,
			url: article?.url || '',
			gnUrl: article?.gnUrl || '',
			source: article?.source || '',
			origin: article?.origin || article?.provider || article?.from || (article?.gnUrl ? 'gn' : ''),
			rank: Number.isFinite(Number(article?.rank))
				? Number(article.rank)
				: (Number.isFinite(Number(article?.position)) ? Number(article.position) : null),
		})
	}
	return { accepted, rejected }
}

export function buildExternalAlternatives(event, results) {
	if (!Array.isArray(results) || !results.length) return []
	let currentSource = normalizeSource(event.source)
	let currentLink = getArticleLink(event)
	let seen = new Set([currentSource])
	let seenTitle = new Set()
	let targetDate = getEventDate(event)
	let items = results
		.filter(item => getArticleLink(item) && item?.source)
		.map(item => ({
			...item,
			url: item?.url || '',
			gnUrl: item?.gnUrl || '',
			level: getAgencyLevel(item.source),
			normalizedSource: normalizeSource(item.source),
			hasDirectUrl: Boolean(item?.url),
			normalizedTitle: getCandidateTitleKey(item),
			origin: item?.origin || item?.provider || item?.from || '',
			parsedDate: parseDate(item?.date),
			rank: Number.isFinite(Number(item?.rank))
				? Number(item.rank)
				: (Number.isFinite(Number(item?.position)) ? Number(item.position) : null),
		}))
	let filtered = []
	for (let item of items) {
		if (!item.normalizedSource) continue
		if (!isWithinDateWindow(targetDate, item.parsedDate)) continue
		if (Number.isFinite(minAgencyLevel) && item.level < minAgencyLevel) continue
		if (seen.has(item.normalizedSource)) {
			let link = getArticleLink(item)
			if (!currentLink || !link || link === currentLink) continue
		}
		let titleKey = item.normalizedTitle
			? `${item.normalizedSource}|${item.normalizedTitle}`
			: `${item.normalizedSource}|__no_title__`
		if (seenTitle.has(titleKey)) continue
		seenTitle.add(titleKey)
		seen.add(item.normalizedSource)
		filtered.push(item)
	}
	return filtered.sort((a, b) => {
		let levelDiff = (b.level - a.level)
		if (levelDiff) return levelDiff
		let directDiff = (b.hasDirectUrl ? 1 : 0) - (a.hasDirectUrl ? 1 : 0)
		if (directDiff) return directDiff
		let rankA = Number.isFinite(Number(a.rank)) ? Number(a.rank) : Number.POSITIVE_INFINITY
		let rankB = Number.isFinite(Number(b.rank)) ? Number(b.rank) : Number.POSITIVE_INFINITY
		return rankA - rankB
	})
}

export function shouldExpandAlternatives(event, alternatives) {
	if (!alternatives.length) return true
	let currentSource = normalizeSource(event.source)
	if (!currentSource) return true
	return alternatives.every(item => normalizeSource(item.source) === currentSource)
}

export function shouldExternalSearch(alternatives) {
	if (!alternatives.length) return true
	return alternatives.every(item => !item.hasDirectUrl)
}

export function getAlternativePool(event) {
	let items = getArticles(event)
		.filter(article => getArticleLink(article) && article?.source)
		.map(article => ({
			source: article.source,
			level: getAgencyLevel(article.source),
		}))
	let seen = new Set()
	let pool = []
	for (let item of items) {
		let key = normalizeSource(item.source)
		if (!key || seen.has(key)) continue
		seen.add(key)
		pool.push(item)
	}
	return pool.sort((a, b) => b.level - a.level)
}

export function mergeArticles(event, articles) {
	if (!Array.isArray(articles) || !articles.length) return 0
	let targetTitle = getTargetTitleKey(event)
	let existing = getArticles(event)
	let combined = existing.slice()
	let seen = new Set()
	for (let item of combined) {
		let link = getArticleLink(item)
		if (!link || !item?.source) continue
		let key = `${normalizeSource(item.source)}|${link}`
		seen.add(key)
	}
	let added = 0
	for (let item of articles) {
		let link = getArticleLink(item)
		if (!link || !item?.source) continue
		if (item.origin === 'sheet') continue
		if (targetTitle) {
			let candidateTitle = getCandidateTitleKey(item)
			if (!titleMatches(targetTitle, candidateTitle)) continue
		}
		let key = `${normalizeSource(item.source)}|${link}`
		if (seen.has(key)) continue
		seen.add(key)
		if (item.origin === 'sheet') item.origin = ''
		combined.push(item)
		added++
	}
	if (added) setArticles(event, combined)
	return added
}
