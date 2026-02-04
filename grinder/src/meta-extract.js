import { JSDOM, VirtualConsole } from 'jsdom'

import { log } from './log.js'
import { decodeHtmlEntities } from './summarize/utils.js'

const jsdomVirtualConsole = new VirtualConsole()
jsdomVirtualConsole.on('jsdomError', () => {})
jsdomVirtualConsole.on('error', () => {})
jsdomVirtualConsole.on('warn', () => {})

const maxMetaHtmlChars = 750_000

function readMeta(doc, selectors) {
	for (let selector of selectors) {
		let node = doc.querySelector(selector)
		if (!node) continue
		let content = node.getAttribute('content') || node.getAttribute('value')
		if (content) return decodeHtmlEntities(content).trim()
	}
	return ''
}

function readMetaAll(doc, selectors) {
	let values = []
	for (let selector of selectors) {
		let nodes = [...doc.querySelectorAll(selector)]
		for (let node of nodes) {
			let content = node.getAttribute('content') || node.getAttribute('value')
			if (content) values.push(decodeHtmlEntities(content).trim())
		}
	}
	return values
}

function readLink(doc, selector) {
	let node = doc.querySelector(selector)
	let href = node?.getAttribute('href')
	return href ? decodeHtmlEntities(href).trim() : ''
}

function splitTags(value) {
	if (!value) return []
	return String(value)
		.split(',')
		.map(item => item.trim())
		.filter(Boolean)
}

function normalizeMetaValue(value, keys = []) {
	if (!value) return ''
	if (typeof value === 'string') return value.trim()
	if (typeof value === 'number' || typeof value === 'boolean') return String(value)
	if (Array.isArray(value)) {
		let items = value.map(item => normalizeMetaValue(item, keys)).filter(Boolean)
		return items.join(', ')
	}
	if (typeof value === 'object') {
		for (let key of keys) {
			let candidate = value?.[key]
			if (typeof candidate === 'string' && candidate.trim()) return candidate.trim()
		}
		let id = value?.['@id'] || value?.id
		if (typeof id === 'string' && id.trim()) return id.trim()
		let url = value?.url || value?.contentUrl || value?.src || value?.href
		if (typeof url === 'string' && url.trim()) return url.trim()
		if (typeof url === 'object') return normalizeMetaValue(url, keys)
	}
	return ''
}

function extractLdJsonMeta(doc) {
	let scripts = [...doc.querySelectorAll('script[type="application/ld+json"]')]
	if (!scripts.length) return {}
	let result = {}
	let collect = node => {
		if (!node || typeof node !== 'object') return
		if (Array.isArray(node)) {
			node.forEach(collect)
			return
		}
		let headline = node.headline || node.name
		let description = node.description
		let keywords = node.keywords
		let datePublished = node.datePublished || node.dateCreated || node.dateModified
		let author = node.author
		let image = node.image
		if (headline && !result.title) result.title = String(headline).trim()
		if (description && !result.description) result.description = String(description).trim()
		if (keywords && !result.keywords) result.keywords = normalizeMetaValue(keywords, ['name', 'text', 'value'])
		if (datePublished && !result.date) result.date = String(datePublished).trim()
		if (author && !result.author) result.author = normalizeMetaValue(author, ['name'])
		if (image && !result.image) result.image = normalizeMetaValue(image, ['url', 'contentUrl'])
		if (node.mainEntityOfPage && !result.canonicalUrl) {
			let url = node.mainEntityOfPage['@id'] || node.mainEntityOfPage.url
			if (url) result.canonicalUrl = String(url).trim()
		}
		for (let value of Object.values(node)) collect(value)
	}
	for (let script of scripts) {
		let raw = script.textContent?.trim()
		if (!raw) continue
		try {
			collect(JSON.parse(raw))
		} catch {
			continue
		}
	}
	return result
}

export function extractMetaFromHtml(html) {
	if (!html) return {}
	try {
		let input = html.length > maxMetaHtmlChars ? html.slice(0, maxMetaHtmlChars) : html
		let dom = new JSDOM(input, { virtualConsole: jsdomVirtualConsole })
		let doc = dom.window.document
		let ld = extractLdJsonMeta(doc)

		let title = ld.title || readMeta(doc, [
			'meta[property="og:title"]',
			'meta[name="twitter:title"]',
			'meta[name="title"]',
			'meta[name="parsely-title"]',
			'meta[name="sailthru.title"]',
		]) || (doc.title ? decodeHtmlEntities(doc.title).trim() : '')

		let description = ld.description || readMeta(doc, [
			'meta[property="og:description"]',
			'meta[name="description"]',
			'meta[name="twitter:description"]',
			'meta[name="sailthru.description"]',
		])

		let keywords = ld.keywords || readMeta(doc, [
			'meta[name="keywords"]',
			'meta[name="news_keywords"]',
			'meta[name="parsely-tags"]',
		])

		let date = ld.date || readMeta(doc, [
			'meta[property="article:published_time"]',
			'meta[name="pubdate"]',
			'meta[name="publishdate"]',
			'meta[name="date"]',
			'meta[name="dc.date"]',
			'meta[name="dc.date.issued"]',
			'meta[name="datepublished"]',
			'meta[property="og:updated_time"]',
		])

		let publishedTime = readMeta(doc, [
			'meta[property="article:published_time"]',
			'meta[name="pubdate"]',
			'meta[name="publishdate"]',
			'meta[name="date"]',
			'meta[name="dc.date"]',
			'meta[name="dc.date.issued"]',
			'meta[name="datepublished"]',
		])

		let modifiedTime = readMeta(doc, [
			'meta[property="article:modified_time"]',
			'meta[property="og:updated_time"]',
			'meta[name="datemodified"]',
			'meta[name="dc.date.modified"]',
		])

		let canonicalUrl = ld.canonicalUrl || readLink(doc, 'link[rel="canonical"]') || readMeta(doc, [
			'meta[property="og:url"]',
			'meta[name="parsely-link"]',
		])

		let image = ld.image || readMeta(doc, [
			'meta[property="og:image"]',
			'meta[property="og:image:url"]',
			'meta[name="twitter:image"]',
			'meta[name="twitter:image:src"]',
		])

		let author = ld.author || readMeta(doc, [
			'meta[name="author"]',
			'meta[property="article:author"]',
			'meta[name="parsely-author"]',
			'meta[name="sailthru.author"]',
			'meta[name="byl"]',
		])

		let siteName = readMeta(doc, [
			'meta[property="og:site_name"]',
			'meta[name="application-name"]',
		])

		let section = readMeta(doc, [
			'meta[property="article:section"]',
			'meta[name="parsely-section"]',
			'meta[name="sailthru.section"]',
			'meta[name="section"]',
		])

		let type = readMeta(doc, [
			'meta[property="og:type"]',
		])

		let locale = readMeta(doc, [
			'meta[property="og:locale"]',
		])

		let tags = []
		let articleTags = readMetaAll(doc, [
			'meta[property="article:tag"]',
		])
		for (let value of articleTags) tags.push(value)

		let extraTags = [
			...splitTags(readMeta(doc, ['meta[name="news_keywords"]'])),
			...splitTags(readMeta(doc, ['meta[name="keywords"]'])),
			...splitTags(readMeta(doc, ['meta[name="parsely-tags"]'])),
			...splitTags(readMeta(doc, ['meta[name="sailthru.tags"]'])),
		]
		tags = [...new Set([...tags, ...extraTags])].filter(Boolean)

		let lang = doc.documentElement?.getAttribute('lang') || ''

		return {
			title: title || '',
			description: description || '',
			keywords: keywords || '',
			date: date || '',
			publishedTime: publishedTime || '',
			modifiedTime: modifiedTime || '',
			canonicalUrl: canonicalUrl || '',
			image: image || '',
			author: author || '',
			siteName: siteName || '',
			section: section || '',
			type: type || '',
			locale: locale || '',
			tags: tags.join(', '),
			lang: lang || '',
		}
	} catch (error) {
		log('meta extract failed', error?.message || error)
		return {}
	}
}
