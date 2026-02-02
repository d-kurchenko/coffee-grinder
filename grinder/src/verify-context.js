import { JSDOM, VirtualConsole } from 'jsdom'

import { fetchArticle } from './fetch-article.js'
import { htmlToText } from './html-to-text.js'
import { log } from './log.js'
import { decodeHtmlEntities, normalizeTitleForSearch } from './summarize/utils.js'
import { verifyContextMaxChars } from '../config/verification.js'

const jsdomVirtualConsole = new VirtualConsole()
jsdomVirtualConsole.on('jsdomError', () => {})
jsdomVirtualConsole.on('error', () => {})
jsdomVirtualConsole.on('warn', () => {})

function readMeta(doc, selectors) {
	for (let selector of selectors) {
		let node = doc.querySelector(selector)
		if (!node) continue
		let content = node.getAttribute('content') || node.getAttribute('value')
		if (content) return decodeHtmlEntities(content).trim()
	}
	return ''
}

function readLink(doc, selector) {
	let node = doc.querySelector(selector)
	let href = node?.getAttribute('href')
	return href ? decodeHtmlEntities(href).trim() : ''
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
		if (headline && !result.title) result.title = String(headline).trim()
		if (description && !result.description) result.description = String(description).trim()
		if (keywords && !result.keywords) result.keywords = Array.isArray(keywords) ? keywords.join(', ') : String(keywords)
		if (datePublished && !result.date) result.date = String(datePublished).trim()
		if (node.mainEntityOfPage && !result.url) {
			let url = node.mainEntityOfPage['@id'] || node.mainEntityOfPage.url
			if (url) result.url = String(url).trim()
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

function extractMeta(html) {
	if (!html) return {}
	try {
		let dom = new JSDOM(html, { virtualConsole: jsdomVirtualConsole })
		let doc = dom.window.document
		let ld = extractLdJsonMeta(doc)
		let title = ld.title || readMeta(doc, [
			'meta[property="og:title"]',
			'meta[name="twitter:title"]',
			'meta[name="title"]',
		]) || (doc.title ? decodeHtmlEntities(doc.title).trim() : '')
		let description = ld.description || readMeta(doc, [
			'meta[property="og:description"]',
			'meta[name="description"]',
			'meta[name="twitter:description"]',
		])
		let keywords = ld.keywords || readMeta(doc, [
			'meta[name="keywords"]',
			'meta[name="news_keywords"]',
		])
		let date = ld.date || readMeta(doc, [
			'meta[property="article:published_time"]',
			'meta[name="pubdate"]',
			'meta[name="publishdate"]',
			'meta[name="date"]',
			'meta[property="og:updated_time"]',
		])
		let canonicalUrl = ld.url || readLink(doc, 'link[rel="canonical"]') || readMeta(doc, [
			'meta[property="og:url"]',
		])
		return {
			title: title || '',
			description: description || '',
			keywords: keywords || '',
			date: date || '',
			canonicalUrl: canonicalUrl || '',
		}
	} catch (error) {
		log('verify context parse failed', error?.message || error)
		return {}
	}
}

function extractTextSnippet(html) {
	if (!html) return ''
	let cleaned = html.replace(/<style[\s\S]*?<\/style>/gi, '')
	let text = htmlToText(cleaned)?.trim() || ''
	if (!text) return ''
	if (!Number.isFinite(verifyContextMaxChars) || verifyContextMaxChars <= 0) return text
	return text.slice(0, verifyContextMaxChars)
}

export async function buildVerifyContext(event) {
	if (event?._verifyContext) return event._verifyContext
	let context = {
		url: event?._originalUrl || event?.url || '',
		gnUrl: event?._originalGnUrl || event?.gnUrl || '',
		title: event?._originalTitleEn || event?._originalTitleRu || event?.titleEn || event?.titleRu || '',
		source: event?._originalSource || event?.source || '',
		date: event?._originalDate || event?.date || '',
		description: '',
		keywords: '',
		textSnippet: '',
	}
	if (context.url) {
		let html = await fetchArticle(context.url)
		if (html) {
			let meta = extractMeta(html)
			if (meta.title) context.title = meta.title
			if (meta.description) context.description = meta.description
			if (meta.keywords) context.keywords = meta.keywords
			if (meta.date) context.date = meta.date
			if (meta.canonicalUrl && !context.url) context.url = meta.canonicalUrl
			context.textSnippet = extractTextSnippet(html)
		}
	}
	context.title = normalizeTitleForSearch(context.title)
	event._verifyContext = context
	return context
}
