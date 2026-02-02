import { fallbackMinAgencyLevel, minAgencyLevel } from '../../config/verification.js'
import { getAgencyLevel, getArticleLink, normalizeSource, normalizeTitleKey } from './utils.js'

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
}

export function getArticles(event) {
	if (Array.isArray(event?._articles)) return event._articles
	if (Array.isArray(event?.articles)) return event.articles
	let parsed = parseArticlesValue(event?.articles)
	if (parsed.length) {
		event._articles = parsed
	}
	return parsed
}

export function getAlternativeArticles(event) {
	let currentSource = normalizeSource(event.source)
	let currentLink = getArticleLink(event)
	let seen = new Set([currentSource])
	let seenTitle = new Set()
	let items = getArticles(event)
		.filter(article => getArticleLink(article) && article?.source)
		.map(article => ({
			...article,
			url: article?.url || '',
			gnUrl: article?.gnUrl || '',
			level: getAgencyLevel(article.source),
			normalizedSource: normalizeSource(article.source),
			hasDirectUrl: Boolean(article?.url),
			normalizedTitle: normalizeTitleKey(article.titleEn || article.titleRu || ''),
		}))
	let filterByLevel = minLevel => {
		let filtered = []
		for (let article of items) {
			if (!article.normalizedSource) continue
			if (seen.has(article.normalizedSource)) {
				let link = getArticleLink(article)
				if (!currentLink || !link || link === currentLink) continue
			}
			if (article.level < minLevel) continue
			let titleKey = article.normalizedTitle
				? `${article.normalizedSource}|${article.normalizedTitle}`
				: `${article.normalizedSource}|__no_title__`
			if (seenTitle.has(titleKey)) continue
			seenTitle.add(titleKey)
			seen.add(article.normalizedSource)
			filtered.push(article)
		}
		return filtered
	}
	let primary = filterByLevel(minAgencyLevel)
	if (primary.length) return primary.sort((a, b) => (b.level - a.level) || ((b.hasDirectUrl ? 1 : 0) - (a.hasDirectUrl ? 1 : 0)))
	if (fallbackMinAgencyLevel < minAgencyLevel) {
		let relaxed = filterByLevel(fallbackMinAgencyLevel)
		return relaxed.sort((a, b) => (b.level - a.level) || ((b.hasDirectUrl ? 1 : 0) - (a.hasDirectUrl ? 1 : 0)))
	}
	return []
}

export function buildExternalAlternatives(event, results) {
	if (!Array.isArray(results) || !results.length) return []
	let currentSource = normalizeSource(event.source)
	let currentLink = getArticleLink(event)
	let seen = new Set([currentSource])
	let seenTitle = new Set()
	let items = results
		.filter(item => getArticleLink(item) && item?.source)
		.map(item => ({
			...item,
			url: item?.url || '',
			gnUrl: item?.gnUrl || '',
			level: getAgencyLevel(item.source),
			normalizedSource: normalizeSource(item.source),
			hasDirectUrl: Boolean(item?.url),
			normalizedTitle: normalizeTitleKey(item.titleEn || item.titleRu || ''),
		}))
	let filtered = []
	for (let item of items) {
		if (!item.normalizedSource) continue
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
	return filtered.sort((a, b) => (b.level - a.level) || ((b.hasDirectUrl ? 1 : 0) - (a.hasDirectUrl ? 1 : 0)))
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
		let key = `${normalizeSource(item.source)}|${link}`
		if (seen.has(key)) continue
		seen.add(key)
		combined.push(item)
		added++
	}
	if (added) setArticles(event, combined)
	return added
}
