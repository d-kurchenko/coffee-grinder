import fs from 'fs'
import { createHash } from 'crypto'

import { sourceFromUrl } from '../external-search.js'
import { decodeHtmlEntities, isBlank, normalizeUrl } from './utils.js'

function extractTitleFromHtml(html) {
	if (!html) return ''
	let metaPatterns = [
		/<meta[^>]+(?:property|name)=["']og:title["'][^>]*>/i,
		/<meta[^>]+(?:property|name)=["']twitter:title["'][^>]*>/i,
		/<meta[^>]+name=["']title["'][^>]*>/i,
	]
	for (let pattern of metaPatterns) {
		let match = html.match(pattern)
		if (!match) continue
		let content = match[0].match(/content=["']([^"']+)["']/i)
		if (content?.[1]) return decodeHtmlEntities(content[1]).trim()
	}
	let title = html.match(/<title[^>]*>([^<]*)<\/title>/i)
	if (title?.[1]) return decodeHtmlEntities(title[1]).trim()
	return ''
}

function stripTrackingParams(url) {
	if (!url) return ''
	try {
		let parsed = new URL(url)
		let params = parsed.searchParams
		let trackingPrefixes = ['utm_', 'gaa_', 'ga_']
		let trackingKeys = new Set([
			'gclid',
			'fbclid',
			'yclid',
			'mc_cid',
			'mc_eid',
			'igshid',
			'cmpid',
			'ref',
			'refsrc',
			'mkt_tok',
		])
		for (let key of [...params.keys()]) {
			let lower = key.toLowerCase()
			if (trackingPrefixes.some(prefix => lower.startsWith(prefix)) || trackingKeys.has(lower)) {
				params.delete(key)
			}
		}
		let query = params.toString()
		parsed.search = query ? `?${query}` : ''
		return parsed.toString()
	} catch {
		return url
	}
}

export function getCacheInfo(event, urlOverride) {
	let override = isBlank(urlOverride) ? '' : normalizeUrl(urlOverride)
	let url = override || normalizeUrl(event?.url || '')
	if (url && !/^[a-z][a-z0-9+.-]*:\/\//i.test(url)) {
		url = `https://${url}`
	}
	if (!url) return null
	let cleaned = stripTrackingParams(url)
	if (!cleaned) return null
	let key = createHash('sha256').update(cleaned).digest('hex')
	return { key, url: cleaned }
}

export function probeCache(event, urlOverride) {
	let cache = getCacheInfo(event, urlOverride)
	if (!cache) {
		return { available: false, reason: 'no_url' }
	}
	let htmlPath = `articles/${cache.key}.html`
	let txtPath = `articles/${cache.key}.txt`
	let hasHtml = fs.existsSync(htmlPath)
	let hasTxt = fs.existsSync(txtPath)
	return {
		available: hasHtml || hasTxt,
		reason: hasHtml || hasTxt ? 'found' : 'missing',
		key: cache.key,
		url: cache.url,
		htmlPath,
		txtPath,
		hasHtml,
		hasTxt,
	}
}

export function backfillMetaFromDisk(event, urlOverride) {
	let changed = false
	let cache = getCacheInfo(event, urlOverride)
	if (!cache) return false
	let htmlPath = `articles/${cache.key}.html`
	if (fs.existsSync(htmlPath)) {
		let html = fs.readFileSync(htmlPath, 'utf8')
		let comment = html.match(/^<!--\s*([\s\S]*?)\s*-->/)
		if (comment?.[1] && isBlank(event.url)) {
			event.url = comment[1].trim()
			changed = true
		}
		let beforeTitle = event.titleEn
		let beforeSource = event.source
		if (isBlank(event.titleEn)) {
			let extracted = extractTitleFromHtml(html)
			if (extracted) event.titleEn = extracted
		}
		if (isBlank(event.source) && event.url && !event.url.includes('news.google.com')) {
			let inferred = sourceFromUrl(event.url)
			if (inferred) event.source = inferred
		}
		if (event.titleEn !== beforeTitle || event.source !== beforeSource) changed = true
	} else if (isBlank(event.source) && event.url && !event.url.includes('news.google.com')) {
		let inferred = sourceFromUrl(event.url)
		if (inferred) {
			event.source = inferred
			changed = true
		}
	}
	return changed
}

export function backfillTextFromDisk(event, urlOverride) {
	if (event?.text?.length) return false
	let cache = getCacheInfo(event, urlOverride)
	if (!cache) return false
	let txtPath = `articles/${cache.key}.txt`
	if (!fs.existsSync(txtPath)) return false
	let raw = fs.readFileSync(txtPath, 'utf8')
	if (!raw) return false
	let [, text] = raw.split(/\n\n/, 2)
	let trimmed = (text || raw).trim()
	if (!trimmed) return false
	event.text = trimmed.slice(0, 30000)
	return true
}

export function readHtmlFromDisk(event, urlOverride) {
	let cache = getCacheInfo(event, urlOverride)
	if (!cache) return ''
	let htmlPath = `articles/${cache.key}.html`
	if (!fs.existsSync(htmlPath)) return ''
	let html = fs.readFileSync(htmlPath, 'utf8')
	if (!html) return ''
	if (html.startsWith('<!--')) {
		let end = html.indexOf('-->')
		if (end !== -1) {
			html = html.slice(end + 3)
		}
	}
	return html
}

export function writeTextCache(event, text, urlOverride) {
	let cache = getCacheInfo(event, urlOverride)
	if (!cache) return false
	let txtPath = `articles/${cache.key}.txt`
	let title = event.titleEn || event.titleRu || ''
	let body = text || ''
	fs.writeFileSync(txtPath, `${title}\n\n${body}`)
	return true
}

export function saveArticle(event, html, text, urlOverride) {
	let cache = getCacheInfo(event, urlOverride)
	if (isBlank(event.titleEn) && html) {
		let extracted = extractTitleFromHtml(html)
		if (extracted) event.titleEn = extracted
	}
	if (isBlank(event.source) && event.url && !event.url.includes('news.google.com')) {
		let inferred = sourceFromUrl(event.url)
		if (inferred) event.source = inferred
	}
	event.text = text.slice(0, 30000)
	if (!cache) return
	fs.writeFileSync(`articles/${cache.key}.html`, `<!--\n${cache.url}\n-->\n${html || ''}`)
	fs.writeFileSync(`articles/${cache.key}.txt`, `${event.titleEn || event.titleRu || ''}\n\n${event.text}`)
}
