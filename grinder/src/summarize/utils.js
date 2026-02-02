import { agencyLevels, defaultAgencyLevel } from '../../config/agencies.js'

export function normalizeKey(value) {
	if (!value) return ''
	return value
		.toLowerCase()
		.replace(/[\u2019'"`.]/g, '')
		.replace(/[\u2013\u2014-]/g, ' ')
		.replace(/^the\s+/, '')
		.replace(/\s+/g, ' ')
		.trim()
}

const agencyLevelsNormalized = Object.fromEntries(
	Object.entries(agencyLevels).map(([key, value]) => [normalizeKey(key), value])
)

export function normalizeSource(source) {
	return normalizeKey(source)
}

export function getAgencyLevel(source) {
	let key = normalizeSource(source)
	return agencyLevelsNormalized[key] ?? defaultAgencyLevel
}

export function normalizeUrl(value) {
	if (!value) return ''
	return String(value).trim()
}

export function isGoogleNewsUrl(url) {
	if (!url) return false
	try {
		let host = new URL(url).hostname.replace(/^www\./, '')
		return host === 'news.google.com'
	} catch {
		return false
	}
}

export function decodeHtmlEntities(value) {
	if (!value) return ''
	let text = String(value)
	text = text
		.replace(/&amp;/gi, '&')
		.replace(/&quot;/gi, '"')
		.replace(/&#39;|&#x27;/gi, "'")
		.replace(/&lt;/gi, '<')
		.replace(/&gt;/gi, '>')
		.replace(/&nbsp;/gi, ' ')
	text = text.replace(/&#x([0-9a-f]+);/gi, (match, hex) => {
		let code = Number.parseInt(hex, 16)
		if (!Number.isFinite(code)) return match
		try {
			return String.fromCodePoint(code)
		} catch {
			return match
		}
	})
	text = text.replace(/&#(\d+);/g, (match, dec) => {
		let code = Number.parseInt(dec, 10)
		if (!Number.isFinite(code)) return match
		try {
			return String.fromCodePoint(code)
		} catch {
			return match
		}
	})
	return text
}

export function normalizeTitleForSearch(title) {
	if (!title) return ''
	let cleaned = decodeHtmlEntities(title)
		.replace(/[“”„«»]/g, '"')
		.replace(/[‘’]/g, "'")
		.replace(/"/g, '')
		.replace(/\s+\|\s+.*$/, '')
		.replace(/\s+-\s+[^-]+$/, '')
	return cleaned.trim()
}

export function normalizeTitleKey(title) {
	let cleaned = normalizeTitleForSearch(title)
	if (!cleaned) return ''
	cleaned = cleaned.replace(/^(live updates:|analysis:|opinion:)\s+/i, '')
	return normalizeKey(cleaned)
}

export function extractSearchTermsFromUrl(url) {
	if (!url) return ''
	try {
		let parsed = new URL(url)
		let slug = parsed.pathname.split('/').filter(Boolean).pop() || ''
		if (!slug) return ''
		if (slug.includes('newsml_')) {
			let idx = slug.lastIndexOf(':0-')
			if (idx !== -1) slug = slug.slice(idx + 3)
			slug = slug.replace(/newsml_[^:-]+[:\d-]*/gi, '')
		}
		slug = slug.replace(/^[^a-z0-9]+/i, '')
		let terms = slug.replace(/[-_]/g, ' ').replace(/\s+/g, ' ').trim()
		return terms
	} catch {
		return ''
	}
}

export function isBlank(value) {
	return !value || String(value).trim().length === 0
}

export const requiredFields = [
	'gnUrl',
	'url',
	'source',
	'titleEn',
	'titleRu',
	'summary',
	'topic',
	'priority',
]

export function missingFields(event) {
	return requiredFields.filter(field => isBlank(event?.[field]))
}

export function isComplete(event) {
	return missingFields(event).length === 0
}

export function getArticleLink(article) {
	return normalizeUrl(article?.gnUrl || article?.url)
}

export function titleFor(event) {
	return event.titleEn || event.titleRu || ''
}
