import fs from 'fs'

import { sourceFromUrl } from '../external-search.js'
import { decodeHtmlEntities, isBlank } from './utils.js'

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

export function backfillMetaFromDisk(event) {
	let changed = false
	let htmlPath = `articles/${event.id}.html`
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

export function backfillTextFromDisk(event) {
	if (event?.text?.length) return false
	let txtPath = `articles/${event.id}.txt`
	if (!fs.existsSync(txtPath)) return false
	let raw = fs.readFileSync(txtPath, 'utf8')
	if (!raw) return false
	let [, text] = raw.split(/\n\n/, 2)
	let trimmed = (text || raw).trim()
	if (!trimmed) return false
	event.text = trimmed.slice(0, 30000)
	return true
}

export function saveArticle(event, html, text) {
	fs.writeFileSync(`articles/${event.id}.html`, `<!--\n${event.url}\n-->\n${html || ''}`)
	if (isBlank(event.titleEn) && html) {
		let extracted = extractTitleFromHtml(html)
		if (extracted) event.titleEn = extracted
	}
	if (isBlank(event.source) && event.url && !event.url.includes('news.google.com')) {
		let inferred = sourceFromUrl(event.url)
		if (inferred) event.source = inferred
	}
	event.text = text.slice(0, 30000)
	fs.writeFileSync(`articles/${event.id}.txt`, `${event.titleEn || event.titleRu || ''}\n\n${event.text}`)
}
