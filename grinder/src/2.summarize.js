import { JSDOM, VirtualConsole } from 'jsdom'

import { log } from './log.js'
import { sleep } from './sleep.js'
import { news, pauseAutoSave, resumeAutoSave, saveRowByIndex, spreadsheetId, spreadsheetMode } from './store.js'
import { topics, topicsMap } from '../config/topics.js'
import { decodeGoogleNewsUrl, getGoogleNewsDecodeCooldownMs } from './google-news.js'
import { fetchArticle, getLastFetchStatus } from './fetch-article.js'
import { htmlToText } from './html-to-text.js'
import { ai } from './ai.js'
import { browseArticle, finalyze } from './browse-article.js'
import { verifyArticle } from './verify-article.js'
import { buildVerifyContext } from './verify-context.js'
import { searchExternal, sourceFromUrl } from './external-search.js'
import { copyFile } from './google-drive.js'
import { coffeeTodayFolderId, newsSheet } from '../config/google-drive.js'
import {
	verifyMode,
	verifyMinConfidence,
	verifyShortThreshold,
	verifyFailOpen,
} from '../config/verification.js'
import { summarizeConfig } from '../config/summarize.js'
import { externalSearch } from '../config/external-search.js'
import {
	getAlternativeArticles,
	buildExternalAlternatives,
	shouldExpandAlternatives,
	shouldExternalSearch,
	getAlternativePool,
	mergeArticles,
	getArticles,
} from './summarize/articles.js'
import { backfillMetaFromDisk, backfillTextFromDisk, saveArticle } from './summarize/disk.js'
import {
	backfillGnUrl,
	buildFallbackSearchQueries,
	hydrateFromGoogleNews,
	searchGoogleNews,
} from './summarize/gn.js'
import { logEvent } from './summarize/logging.js'
import { searchAgencyArticles } from './summarize/agency-search.js'
import { logging } from '../config/logging.js'
import {
	isBlank,
	isGoogleNewsUrl,
	missingFields,
	normalizeSource,
	normalizeUrl,
	titleFor,
} from './summarize/utils.js'

const minTextLength = 400
const maxHtmlToTextChars = 4_000_000
const fetchAttempts = 2
const verifyStatusColumn = 'verifyStatus'
const articlesColumn = 'articles'
const jsdomVirtualConsole = new VirtualConsole()
jsdomVirtualConsole.on('jsdomError', () => {})
jsdomVirtualConsole.on('error', () => {})
jsdomVirtualConsole.on('warn', () => {})

function shouldVerify({ isFallback, textLength }) {
	if (verifyMode === 'always') return true
	if (verifyMode === 'fallback') return isFallback
	if (verifyMode === 'short') return textLength < verifyShortThreshold
	return false
}

function cloneEvent(event) {
	let copy = { ...event }
	if (Array.isArray(event?._articles)) {
		copy._articles = event._articles.map(item => ({ ...item }))
	}
	if (Array.isArray(event?.articles)) {
		copy.articles = event.articles.map(item => ({ ...item }))
	}
	return copy
}

function commitEvent(target, source) {
	for (let [key, value] of Object.entries(source || {})) {
		target[key] = value
	}
}

function ensureColumns(columns) {
	if (!news?.headers) return
	columns.forEach(column => {
		if (!news.headers.includes(column)) {
			news.headers.push(column)
		}
	})
}

function applyVerifyStatus(event, verify) {
	if (!verify) return
	let status = verify.status || (verify.ok ? 'ok' : 'mismatch')
	event._verifyStatus = status
}

function captureOriginalContext(event, base) {
	let source = base || event || {}
	if (!event._originalUrl && !isBlank(source.url)) event._originalUrl = source.url
	if (!event._originalGnUrl && !isBlank(source.gnUrl)) event._originalGnUrl = source.gnUrl
	if (!event._originalTitleEn && !isBlank(source.titleEn)) event._originalTitleEn = source.titleEn
	if (!event._originalTitleRu && !isBlank(source.titleRu)) event._originalTitleRu = source.titleRu
	if (!event._originalSource && !isBlank(source.source)) event._originalSource = source.source
	if (!event._originalDate && !isBlank(source.date)) event._originalDate = source.date
}

function resetTextFields(event) {
	event.text = ''
	event.summary = ''
	event.titleRu = ''
	event.topic = ''
	event.priority = ''
	event.aiTopic = ''
	event.aiPriority = ''
}

function setOriginalUrlIfMissing(event) {
	if (!event._originalUrl && !isBlank(event.url)) event._originalUrl = event.url
}

function sanitizeContentText(text) {
	if (!text) return ''
	let cleaned = String(text)
		.replace(/<[^>]+>/g, ' ')
		.replace(/[\u0000-\u001F\u007F]/g, ' ')
		.replace(/\s+/g, ' ')
		.trim()
	let maxChars = Number.isFinite(logging.contentTextMaxChars) ? logging.contentTextMaxChars : 0
	let hardLimit = Number.isFinite(logging.maxDataStringLength) ? logging.maxDataStringLength : 0
	let limit = maxChars > 0 ? maxChars : 0
	if (hardLimit > 0 && limit > 0) limit = Math.min(limit, hardLimit)
	if (limit > 0 && cleaned.length > limit) {
		let suffix = `... (${cleaned.length - limit} more chars)`
		cleaned = cleaned.slice(0, Math.max(0, limit - suffix.length)) + suffix
	}
	return cleaned
}

function setContentSource(event, { url, source, method, isFallback }) {
	if (event._contentUrl) return
	event._contentUrl = url || ''
	event._contentSource = source || ''
	event._contentMethod = method || ''
	event._contentIsFallback = Boolean(isFallback)
	let contentText = ''
	if (logging.includeContentText) {
		contentText = sanitizeContentText(event.text || '')
	}
	logEvent(event, {
		phase: 'content_selected',
		status: 'ok',
		contentUrl: event._contentUrl,
		contentSource: event._contentSource,
		contentMethod: event._contentMethod,
		contentIsFallback: event._contentIsFallback,
		originalUrl: event._originalUrl || '',
		originalGnUrl: event._originalGnUrl || '',
		contentText,
	}, `#${event.id} content selected (${event._contentMethod})`, 'info')
}

function truncate(text, max = 220) {
	if (!text) return ''
	if (text.length <= max) return text
	return text.slice(0, max - 3) + '...'
}

async function decodeUrl(gnUrl, last) {
	if (gnUrl && isGoogleNewsUrl(gnUrl)) {
		let cooldownMs = getGoogleNewsDecodeCooldownMs()
		if (cooldownMs > 0) {
			log('google news decode cooldown active', Math.ceil(cooldownMs / 1000), 's')
			return ''
		}
	}
	await sleep(last.urlDecode.time + last.urlDecode.delay - Date.now())
	let maxDelay = Number.isFinite(last.urlDecode.maxDelay)
		? last.urlDecode.maxDelay
		: last.urlDecode.delay
	last.urlDecode.delay = Math.min(last.urlDecode.delay + last.urlDecode.increment, maxDelay)
	last.urlDecode.time = Date.now()
	log('Decoding URL...')
	if (!gnUrl) return ''
	if (!isGoogleNewsUrl(gnUrl)) return gnUrl
	return await decodeGoogleNewsUrl(gnUrl)
}

function extractJsonText(document) {
	let scripts = [...document.querySelectorAll('script[type="application/ld+json"]')]
	if (!scripts.length) return
	let buckets = { body: [], text: [], desc: [] }
	let seen = new Set()
	let collect = node => {
		if (!node || seen.has(node)) return
		if (typeof node === 'string') return
		if (Array.isArray(node)) {
			node.forEach(collect)
			return
		}
		if (typeof node !== 'object') return
		seen.add(node)
		if (typeof node.articleBody === 'string') buckets.body.push(node.articleBody)
		if (typeof node.text === 'string') buckets.text.push(node.text)
		if (typeof node.description === 'string') buckets.desc.push(node.description)
		Object.values(node).forEach(collect)
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
	let pick = list => list.sort((a, b) => b.length - a.length)[0]
	let candidate =
		pick(buckets.body) ||
		pick(buckets.text) ||
		pick(buckets.desc)
	if (candidate && candidate.length > minTextLength) return candidate.trim()
}

function extractDomText(document) {
	const selectors = [
		'[itemprop="articleBody"]',
		'article',
		'main',
		'.article-body',
		'.article-body__content',
		'.story-body',
		'.content__article-body',
		'.ArticleBody',
		'.ArticleBody-articleBody',
	]
	let best = ''
	for (let selector of selectors) {
		let nodes = [...document.querySelectorAll(selector)]
		for (let node of nodes) {
			let text = safeHtmlToText(node.innerHTML || '')
			if (text && text.length > best.length) {
				best = text
			}
		}
	}
	if (best.length > minTextLength) return best
}

function stripHtmlFast(html, limit = maxHtmlToTextChars) {
	let input = html || ''
	if (limit && input.length > limit) input = input.slice(0, limit)
	let text = input.replace(/<[^>]+>/g, ' ')
	text = text.replace(/\s+/g, ' ').trim()
	return text
}

function safeHtmlToText(html) {
	if (!html) return ''
	if (html.length > maxHtmlToTextChars) {
		log('html too large for html-to-text', html.length, 'chars')
		return stripHtmlFast(html)
	}
	try {
		return htmlToText(html)?.trim() || ''
	} catch (error) {
		log('html-to-text failed', error?.message || error)
		return stripHtmlFast(html)
	}
}

function extractText(html) {
	if (!html) return
	let cleaned = html.replace(/<style[\s\S]*?<\/style>/gi, '')
	if (!/<[a-z][\s\S]*>/i.test(cleaned)) {
		let plain = cleaned.trim()
		if (plain.length > minTextLength) return plain
	}
	try {
		let dom = new JSDOM(cleaned, { virtualConsole: jsdomVirtualConsole })
		let doc = dom.window.document
		let jsonText = extractJsonText(doc)
		if (jsonText) return jsonText
		let domText = extractDomText(doc)
		if (domText) return domText
	} catch {}
	let text = safeHtmlToText(cleaned)
	if (!text || text.length <= minTextLength) return
	return text
}

async function verifyText({ event, url, text, isFallback, method, attempt, last }) {
	if (!shouldVerify({ isFallback, textLength: text.length })) {
		logEvent(event, {
			phase: 'verify',
			status: 'skipped',
			method,
			attempt,
			textLength: text.length,
		}, `#${event.id} verify skipped (${method})`, 'info')
		return { ok: true, status: 'skipped', verified: false, skipped: true }
	}
	if (isBlank(event._originalUrl) && !isBlank(event.gnUrl)) {
		let decoded = await decodeUrl(event.gnUrl, last)
		if (decoded && isBlank(event.url)) {
			event.url = decoded
		}
		setOriginalUrlIfMissing(event)
	}
	await sleep(last.verify.time + last.verify.delay - Date.now())
	last.verify.time = Date.now()
	let context = await buildVerifyContext(event)
	let result = await verifyArticle({
		original: context,
		candidate: { url, text },
		minConfidence: verifyMinConfidence,
		failOpen: verifyFailOpen,
		debug: logging.includeVerifyPrompt,
		debugMaxChars: logging.verifyPromptMaxChars,
	})
	applyVerifyStatus(event, result)
	let status = result?.status || (result?.ok ? 'ok' : (result?.error ? 'error' : 'mismatch'))
	let summarySnippet = result?.pageSummary ? ` | ${truncate(result.pageSummary)}` : ''
	let errorMessage = result?.error ? String(result.error?.message || result.error) : undefined
	let statusMessage = status === 'unverified' ? 'unverified (gpt unavailable)' : status
	let verifyDebug = result?.debug
	logEvent(event, {
		phase: 'verify',
		status,
		method,
		attempt,
		textLength: text.length,
		match: result?.match,
		confidence: result?.confidence,
		reason: result?.reason,
		pageSummary: result?.pageSummary,
		verified: result?.verified,
		error: errorMessage,
		tokens: result?.tokens,
		verifyModel: verifyDebug?.model,
		verifyTemperature: verifyDebug?.temperature,
		verifyUseSearch: verifyDebug?.useSearch,
		verifyFallback: result?.fallbackUsed || verifyDebug?.fallbackUsed,
		verifyFallbackMaxChars: verifyDebug?.fallbackMaxChars,
		verifyFallbackContextMaxChars: verifyDebug?.fallbackContextMaxChars,
		verifySystem: verifyDebug?.system,
		verifyPrompt: verifyDebug?.prompt,
	}, `#${event.id} verify ${statusMessage} (${method})${summarySnippet}`, result?.ok ? 'ok' : 'warn')
	return result
}

async function fetchTextWithRetry(event, url, last, { isFallback = false } = {}) {
	let foundText = false
	const browseHedgeDelayMs = 1500
	for (let attempt = 1; attempt <= fetchAttempts; attempt++) {
		let mismatchResult = null
		let mismatchHtml = null
		let mismatchText = null
		let browsePromise = null
		let browseStarted = false
		let startBrowse = async () => {
			browseStarted = true
			let html = ''
			let browseFailed = false
			try {
				html = await browseArticle(url, { ignoreCooldown: !isFallback })
			} catch (error) {
				if (error?.code === 'BROWSER_CLOSED') throw error
				if (error?.code === 'CAPTCHA') {
					logEvent(event, {
						phase: 'browse',
						method: 'browse',
						status: 'captcha',
						attempt,
					}, `#${event.id} browse captcha`, 'warn')
					return { html: '', browseFailed: false, aborted: true }
				}
				if (error?.code === 'TIMEOUT') {
					logEvent(event, {
						phase: 'browse',
						method: 'browse',
						status: 'timeout',
						attempt,
					}, `#${event.id} browse timeout`, 'warn')
					return { html: '', browseFailed: true, aborted: true }
				}
				browseFailed = true
				logEvent(event, {
					phase: 'browse',
					method: 'browse',
					status: 'error',
					error: error?.message || String(error),
					errorCode: error?.code || '',
				}, `#${event.id} browse failed`, 'warn')
				html = ''
			}
			return { html, browseFailed }
		}
		let browseTimer = setTimeout(() => {
			if (!browseStarted) browsePromise = startBrowse()
		}, browseHedgeDelayMs)
		let html = await fetchArticle(url)
		clearTimeout(browseTimer)
		let lastStatus = getLastFetchStatus(url)
		let text = extractText(html)
		if (!text && lastStatus === 429) {
			logEvent(event, {
				phase: 'fetch',
				method: 'fetch',
				status: 'rate_limited',
				attempt,
				httpStatus: 429,
			}, `#${event.id} fetch rate-limited (429)`, 'warn')
			return { ok: false, rateLimited: true }
		}
		if (text) {
			foundText = true
			logEvent(event, {
				phase: 'fetch',
				method: 'fetch',
				status: 'ok',
				attempt,
				textLength: text.length,
			}, `#${event.id} fetch ok (${attempt}/${fetchAttempts})`, 'ok')
			let verify = await verifyText({ event, url, text, isFallback, method: 'fetch', attempt, last })
			if (verify?.ok) {
				return { ok: true, html, text, verify, method: 'fetch', url }
			}
			if (verify?.status === 'mismatch') {
				mismatchResult = verify
				mismatchHtml = html
				mismatchText = text
			}
		} else {
			logEvent(event, {
				phase: 'fetch',
				method: 'fetch',
				status: 'no_text',
				attempt,
			}, `#${event.id} fetch no text (${attempt}/${fetchAttempts})`, 'warn')
		}

		if (mismatchResult && !summarizeConfig.browseOnMismatch) {
			return { ok: false, mismatch: true, verify: mismatchResult, html: mismatchHtml, text: mismatchText }
		}

		if (!browsePromise) browsePromise = startBrowse()
		let browseResult = await browsePromise
		html = browseResult?.html || ''
		let browseFailed = Boolean(browseResult?.browseFailed)
		text = extractText(html)
		if (text) {
			foundText = true
			logEvent(event, {
				phase: 'fetch',
				method: 'browse',
				status: 'ok',
				attempt,
				textLength: text.length,
			}, `#${event.id} browse ok (${attempt}/${fetchAttempts})`, 'ok')
			let verify = await verifyText({ event, url, text, isFallback, method: 'browse', attempt, last })
			if (verify?.ok) {
				return { ok: true, html, text, verify, method: 'browse', url }
			}
			if (verify?.status === 'mismatch') {
				mismatchResult = verify
				mismatchHtml = html
				mismatchText = text
			}
		} else if (!browseFailed) {
			logEvent(event, {
				phase: 'fetch',
				method: 'browse',
				status: 'no_text',
				attempt,
			}, `#${event.id} browse no text (${attempt}/${fetchAttempts})`, 'warn')
		}

		if (mismatchResult) {
			return { ok: false, mismatch: true, verify: mismatchResult, html: mismatchHtml, text: mismatchText }
		}
		log(`article text missing (${attempt}/${fetchAttempts})`)
	}
	if (!foundText) {
		logEvent(event, {
			phase: 'fetch',
			status: 'no_text',
			attempts: fetchAttempts,
		}, `#${event.id} no text after ${fetchAttempts} attempts`, 'warn')
	}
}

export async function summarize() {
	pauseAutoSave()
	try {
		log(`[info] sheet read/write: ${spreadsheetId} (sheet=${newsSheet}, mode=${spreadsheetMode})`)
		ensureColumns([
			'titleEn',
			'titleRu',
			'gnUrl',
			'alternativeUrl',
			'url',
			'source',
			articlesColumn,
			verifyStatusColumn,
		])

		let list = news.filter(e => String(e[verifyStatusColumn] || '').toLowerCase() !== 'ok')

		let stats = { ok: 0, fail: 0 }
		let failures = []
		let externalSearchWarned = false
		let last = {
			urlDecode: { time: 0, delay: 30e3, increment: 1000, maxDelay: 60e3 },
			ai: { time: 0, delay: 0 },
			verify: { time: 0, delay: 1000 },
			gnSearch: { time: 0, delay: 1000, increment: 0 },
		}
		let backfilled = 0
		let backfilledGn = 0
		for (let i = 0; i < list.length; i++) {
			let base = list[i]
			if (String(base?.[verifyStatusColumn] || '').toLowerCase() === 'ok') {
				log(`#${base.id || i + 1} skipped (verifyStatus=ok)`)
				continue
			}
			let e = cloneEvent(base)
			let rowIndex = news.indexOf(base) + 1
			if (!e.id) e.id = base.id || rowIndex
			captureOriginalContext(e, base)
			if (backfillMetaFromDisk(e)) backfilled++
			backfillTextFromDisk(e)
			if (e.text?.length > minTextLength) {
				let verify = await verifyText({
					event: e,
					url: e._originalUrl || e.url || '',
					text: e.text,
					isFallback: false,
					method: 'cache',
					attempt: 0,
					last,
				})
				if (verify?.ok) {
					applyVerifyStatus(e, verify)
					setContentSource(e, {
						url: e._originalUrl || e.url || '',
						source: e._originalSource || e.source || '',
						method: 'cache',
						isFallback: false,
					})
				} else {
					logEvent(e, {
						phase: 'cache_verify',
						status: 'mismatch',
						reason: verify?.reason,
						pageSummary: verify?.pageSummary,
					}, `#${e.id} cached text mismatch`, 'warn')
					resetTextFields(e)
				}
			}
			if (isBlank(e.gnUrl)) {
				if (await backfillGnUrl(e, last, { logEvent })) backfilledGn++
			}
			if (isBlank(e.gnUrl) || isBlank(e.titleEn) || isBlank(e.source)) {
				await hydrateFromGoogleNews(e, last, { decodeUrl, logEvent })
			}
			captureOriginalContext(e, e)
			e.url = normalizeUrl(e.url)
			e.gnUrl = normalizeUrl(e.gnUrl)
			let needsTextFields = isBlank(e.summary) || isBlank(e.titleRu) || isBlank(e.topic) || isBlank(e.priority)
			let hasText = e.text?.length > minTextLength
			log(`\n#${e.id} [${i + 1}/${list.length}]`, titleFor(e))

			if ((hasText || !needsTextFields) && isBlank(e.url) && !isBlank(e.gnUrl)) {
				let decoded = await decodeUrl(e.gnUrl, last)
				if (decoded) {
					e.url = decoded
					setOriginalUrlIfMissing(e)
					logEvent(e, {
						phase: 'decode_url',
						status: 'ok',
						url: e.url,
					}, `#${e.id} url decoded`, 'ok')
				} else {
					logEvent(e, {
						phase: 'decode_url',
						status: 'fail',
					}, `#${e.id} url decode failed`, 'warn')
				}
			}

			if (isBlank(e.source) && e.url && !e.url.includes('news.google.com')) {
				let inferred = sourceFromUrl(e.url)
				if (inferred) e.source = inferred
			}

			if (needsTextFields && !hasText) {
				if (!e.url /*&& !restricted.includes(e.source)*/) {
					e.url = await decodeUrl(e.gnUrl, last)
					if (!e.url) {
						logEvent(e, {
							phase: 'decode_url',
							status: 'fail',
						}, `#${e.id} url decode failed`, 'warn')
						await sleep(5 * 60e3)
						i--
						continue
					}
					setOriginalUrlIfMissing(e)
					logEvent(e, {
						phase: 'decode_url',
						status: 'ok',
						url: e.url,
					}, `#${e.id} url decoded`, 'ok')
					log('got', e.url)
				}

				let fetched = false
				if (e.url) {
					if (isBlank(e.source) && e.url && !e.url.includes('news.google.com')) {
						let inferred = sourceFromUrl(e.url)
						if (inferred) e.source = inferred
					}
					log('Fetching', e.source || '', 'article...')
					let result = await fetchTextWithRetry(e, e.url, last)
					if (result?.ok) {
						log('got', result.text.length, 'chars')
						saveArticle(e, result.html, result.text)
						if (isBlank(e.gnUrl)) {
							await backfillGnUrl(e, last, { logEvent })
						}
						applyVerifyStatus(e, result.verify)
						setContentSource(e, {
							url: result.url || e.url || '',
							source: e.source || '',
							method: result.method || 'fetch',
							isFallback: false,
						})
						fetched = true
					} else if (result?.mismatch) {
						logEvent(e, {
							phase: 'verify_mismatch',
							status: 'fail',
							pageSummary: result?.verify?.pageSummary,
							reason: result?.verify?.reason,
						}, `#${e.id} text mismatch, switching to fallback`, 'warn')
					}
				}

				if (!fetched) {
					let forcedSearch = false
					while (!fetched) {
						let alternatives = getAlternativeArticles(e)
						if (!alternatives.length) {
							await hydrateFromGoogleNews(e, last, { decodeUrl, logEvent })
							alternatives = getAlternativeArticles(e)
						}
						let shouldExpand = shouldExpandAlternatives(e, alternatives)
						let externalResults = []
						if (shouldExpand && !e._gnExpanded) {
							let queries = buildFallbackSearchQueries(e)
							if (queries.length) {
								let totalAdded = 0
								let usedQuery = ''
								for (let query of queries) {
									let results = await searchGoogleNews(query, last)
									let extra = results.map(item => ({
										titleEn: item.titleEn || '',
										gnUrl: item.gnUrl || '',
										url: item.url || '',
										source: item.source || '',
									})).filter(item => item.source && (item.gnUrl || item.url))
									let added = mergeArticles(e, extra)
									if (added) {
										totalAdded += added
										usedQuery = query
										break
									}
								}
								e._gnExpanded = true
								logEvent(e, {
									phase: 'gn_search_expand',
									status: totalAdded ? 'ok' : 'empty',
									query: usedQuery || queries[0],
									queries,
									added: totalAdded,
									total: getArticles(e).length,
								}, `#${e.id} google news expand ${totalAdded ? `added ${totalAdded}` : 'no results'}`, totalAdded ? 'info' : 'warn')
								alternatives = getAlternativeArticles(e)
								shouldExpand = shouldExpandAlternatives(e, alternatives)
							}
						}
						if (shouldExpand && !e._agencyExpanded) {
							let results = await searchAgencyArticles(e, last)
							let added = mergeArticles(e, results)
							e._agencyExpanded = true
							logEvent(e, {
								phase: 'agency_search',
								status: added ? 'ok' : 'empty',
								added,
								total: getArticles(e).length,
							}, `#${e.id} agency search ${added ? `added ${added}` : 'no results'}`, added ? 'info' : 'warn')
							alternatives = getAlternativeArticles(e)
							shouldExpand = shouldExpandAlternatives(e, alternatives)
						}
						let shouldExternal = shouldExternalSearch(alternatives)
						if (shouldExternal && externalSearch?.enabled && !externalSearch.apiKey && !externalSearchWarned) {
							log('external search disabled (missing SEARCH_API_KEY)')
							externalSearchWarned = true
						}
						if (shouldExternal && !e._externalExpanded && externalSearch?.enabled && externalSearch.apiKey) {
							let queries = buildFallbackSearchQueries(e)
							if (queries.length) {
								let totalAdded = 0
								let usedQuery = ''
								for (let query of queries) {
									let results = await searchExternal(query)
									if (results.length) externalResults = results
									let added = mergeArticles(e, results)
									if (added) {
										totalAdded += added
										usedQuery = query
										break
									}
								}
								e._externalExpanded = true
								logEvent(e, {
									phase: 'external_search',
									status: totalAdded ? 'ok' : 'empty',
									provider: externalSearch.provider,
									query: usedQuery || queries[0],
									queries,
									added: totalAdded,
									total: getArticles(e).length,
								}, `#${e.id} external search ${totalAdded ? `added ${totalAdded}` : 'no results'}`, totalAdded ? 'info' : 'warn')
								alternatives = getAlternativeArticles(e)
								shouldExpand = shouldExpandAlternatives(e, alternatives)
							}
						}
						if (!alternatives.length && externalResults.length) {
							alternatives = buildExternalAlternatives(e, externalResults)
						}
						if (alternatives.length) {
							let bySource = new Map()
							for (let alt of alternatives) {
								let key = normalizeSource(alt.source)
								if (!key) continue
								let existing = bySource.get(key)
								if (!existing) {
									bySource.set(key, { source: alt.source, level: alt.level, count: 1 })
								} else {
									existing.count += 1
									existing.level = Math.max(existing.level, alt.level)
								}
							}
							let sourceList = [...bySource.values()].sort((a, b) => b.level - a.level)
							let listForLog = sourceList.slice(0, 12).map(item => `${item.source}(${item.level})${item.count > 1 ? `x${item.count}` : ''}`).join(', ')
							let moreCount = sourceList.length - 12
							logEvent(e, {
								phase: 'fallback_candidates',
								status: 'ok',
								candidates: sourceList.map(a => ({ source: a.source, level: a.level, count: a.count })),
							}, `#${e.id} fallback candidates: ${listForLog}${moreCount > 0 ? ` ... +${moreCount}` : ''}`, 'info')
						} else {
							logEvent(e, {
								phase: 'fallback_candidates',
								status: 'empty',
							}, `#${e.id} no fallback candidates`, 'warn')
							let pool = getAlternativePool(e)
							if (pool.length) {
								logEvent(e, {
									phase: 'fallback_pool',
									status: 'ok',
									candidates: pool.map(a => ({ source: a.source, level: a.level })),
								}, `#${e.id} fallback pool: ${pool.map(a => `${a.source}(${a.level})`).join(', ')}`, 'info')
							}
						}
						let pendingDecode = []
						const tryAlternative = async (alt, { allowWait }) => {
							log('Trying alternative source', alt.source, `(level ${alt.level})...`)
							let altUrl = normalizeUrl(alt.url)
							let decodeMethod = altUrl ? 'direct' : 'gn'
							if (!altUrl && alt.gnUrl) {
								if (!allowWait) {
									let waitMs = (last.urlDecode.time + last.urlDecode.delay) - Date.now()
									if (waitMs > 0) {
										logEvent(e, {
											phase: 'fallback_decode',
											status: 'deferred',
											candidateSource: alt.source,
											level: alt.level,
											method: decodeMethod,
											waitMs,
										}, `#${e.id} fallback decode deferred (${alt.source})`, 'info')
										return { deferred: true }
									}
								}
								altUrl = await decodeUrl(alt.gnUrl, last)
							}
							if (!altUrl) {
								logEvent(e, {
									phase: 'fallback_decode',
									status: 'fail',
									candidateSource: alt.source,
									level: alt.level,
									method: decodeMethod,
								}, `#${e.id} fallback decode failed (${alt.source})`, 'warn')
								return { fetched: false }
							}
							logEvent(e, {
								phase: 'fallback_decode',
								status: 'ok',
								candidateSource: alt.source,
								level: alt.level,
								method: decodeMethod,
								url: altUrl,
							}, `#${e.id} fallback url decoded (${alt.source})`, 'ok')
							let result = await fetchTextWithRetry(e, altUrl, last, { isFallback: true })
							if (result?.ok) {
								if (isBlank(e.source) && alt.source) e.source = alt.source
								if (isBlank(e.gnUrl) && !isBlank(alt.gnUrl)) e.gnUrl = alt.gnUrl
								if (isBlank(e.titleEn) && !isBlank(alt.titleEn)) e.titleEn = alt.titleEn
								if (isBlank(e.url)) e.url = altUrl
								if (isBlank(e.gnUrl) && isBlank(e.alternativeUrl) && !isBlank(altUrl)) {
									let originalUrl = normalizeUrl(e.url || '')
									if (!originalUrl || originalUrl !== altUrl) {
										e.alternativeUrl = altUrl
									}
								}
								setOriginalUrlIfMissing(e)
								log('got', result.text.length, 'chars')
								saveArticle(e, result.html, result.text)
								if (isBlank(e.gnUrl)) {
									await backfillGnUrl(e, last, { logEvent })
								}
								applyVerifyStatus(e, result.verify)
								setContentSource(e, {
									url: result.url || altUrl || '',
									source: e.source || alt.source || '',
									method: result.method || 'fetch',
									isFallback: true,
								})
								fetched = true
								logEvent(e, {
									phase: 'fallback_selected',
									status: 'ok',
									candidateSource: alt.source,
									level: alt.level,
								}, `#${e.id} fallback selected ${alt.source}`, 'ok')
								return { fetched: true }
							} else if (result?.mismatch) {
								logEvent(e, {
									phase: 'fallback_verify_mismatch',
									status: 'fail',
									candidateSource: alt.source,
									level: alt.level,
									pageSummary: result?.verify?.pageSummary,
									reason: result?.verify?.reason,
								}, `#${e.id} fallback text mismatch (${alt.source})`, 'warn')
							}
							return { fetched: false }
						}

						for (let j = 0; j < alternatives.length; j++) {
							let alt = alternatives[j]
							let res = await tryAlternative(alt, { allowWait: false })
							if (res?.deferred) {
								pendingDecode.push(alt)
								continue
							}
							if (res?.fetched) break
						}
						if (!fetched && pendingDecode.length) {
							for (let alt of pendingDecode) {
								let res = await tryAlternative(alt, { allowWait: true })
								if (res?.fetched) break
							}
						}
						if (fetched) break
						if (forcedSearch) {
							logEvent(e, {
								phase: 'fallback_failed',
								status: 'fail',
							}, `#${e.id} fallback exhausted`, 'warn')
							break
						}

						forcedSearch = true
						let forcedQueries = buildFallbackSearchQueries(e)
						let forcedGnAdded = 0
						let forcedGnQuery = ''
						if (forcedQueries.length) {
							for (let query of forcedQueries) {
								let results = await searchGoogleNews(query, last)
								let extra = results.map(item => ({
									titleEn: item.titleEn || '',
									gnUrl: item.gnUrl || '',
									url: item.url || '',
									source: item.source || '',
								})).filter(item => item.source && (item.gnUrl || item.url))
								let added = mergeArticles(e, extra)
								if (added) {
									forcedGnAdded += added
									forcedGnQuery = query
									break
								}
							}
							logEvent(e, {
								phase: 'gn_search_forced',
								status: forcedGnAdded ? 'ok' : 'empty',
								query: forcedGnQuery || forcedQueries[0],
								queries: forcedQueries,
								added: forcedGnAdded,
								total: getArticles(e).length,
							}, `#${e.id} forced google news search ${forcedGnAdded ? `added ${forcedGnAdded}` : 'no results'}`, forcedGnAdded ? 'info' : 'warn')
						}

						if (externalSearch?.enabled && !externalSearch.apiKey && !externalSearchWarned) {
							log('external search disabled (missing SEARCH_API_KEY)')
							externalSearchWarned = true
						}
						if (externalSearch?.enabled && externalSearch.apiKey && forcedQueries.length) {
							let totalAdded = 0
							let usedQuery = ''
							for (let query of forcedQueries) {
								let results = await searchExternal(query)
								let added = mergeArticles(e, results)
								if (added) {
									totalAdded += added
									usedQuery = query
									break
								}
							}
							logEvent(e, {
								phase: 'external_search_forced',
								status: totalAdded ? 'ok' : 'empty',
								provider: externalSearch.provider,
								query: usedQuery || forcedQueries[0],
								queries: forcedQueries,
								added: totalAdded,
								total: getArticles(e).length,
							}, `#${e.id} forced external search ${totalAdded ? `added ${totalAdded}` : 'no results'}`, totalAdded ? 'info' : 'warn')
						}
					}
				}
			}

			if (needsTextFields && e.text?.length > minTextLength) {
				await sleep(last.ai.time + last.ai.delay - Date.now())
				last.ai.time = Date.now()
				log('Summarizing', e.text.length, 'chars...')
				let summarizeStart = Date.now()
				let res = await ai(e)
				let summarizeMs = Date.now() - summarizeStart
				logEvent(e, {
					phase: 'summarize',
					status: res ? 'ok' : 'empty',
					durationMs: summarizeMs,
					inputChars: e.text?.length || 0,
					outputChars: res?.summary?.length || 0,
				}, `#${e.id} summarize ${summarizeMs}ms`, res ? 'info' : 'warn')
				if (res) {
					last.ai.delay = res.delay
					e.topic ||= topicsMap[res.topic]
					e.priority ||= res.priority
					e.titleRu ||= res.titleRu
					if (isBlank(e.summary)) e.summary = res.summary
					if (isBlank(e.aiTopic)) e.aiTopic = topicsMap[res.topic]
					if (isBlank(e.aiPriority)) e.aiPriority = res.priority
				}
			}

			if (!e.summary) {
				logEvent(e, {
					phase: 'summary',
					status: 'missing',
				}, `#${e.id} summary missing`, 'warn')
			}
			if (isBlank(e.gnUrl) && !isBlank(base.gnUrl)) {
				e.gnUrl = base.gnUrl
			}
			let missing = missingFields(e)
			let complete = missing.length === 0
			let verifiedOk = e._verifyStatus === 'ok' || e._verifyStatus === 'skipped'
			e[verifyStatusColumn] = complete && verifiedOk ? 'ok' : ''
			if (!complete || !verifiedOk) {
				failures.push({
					id: e.id,
					title: titleFor(e),
					source: e.source || '',
					url: e.url || '',
					phase: e._lastPhase || '',
					status: e._lastStatus || '',
					method: e._lastMethod || '',
					reason: missing.length
						? `missing: ${missing.join(', ')}`
						: (verifiedOk ? (e._lastReason || '') : `verify status: ${e._verifyStatus || 'unknown'}`),
				})
			}
			if (complete && verifiedOk) stats.ok++
			else stats.fail++
			commitEvent(base, e)
			if (rowIndex > 0) {
				await saveRowByIndex(rowIndex + 1, base)
			} else {
				log(`[warn] #${e.id} row index not found; save skipped`)
			}
		}
		let order = e => (+e.sqk || 999) * 1000 + (topics[e.topic]?.id ?? 99) * 10 + (+e.priority || 10)
		news.sort((a, b) => order(a) - order(b))

		if (failures.length) {
			let limit = summarizeConfig.failSummaryLimit || 0
			log('\nFailed rows:', failures.length)
			let items = limit > 0 ? failures.slice(0, limit) : failures
			for (let item of items) {
				let meta = [item.phase, item.status, item.method].filter(Boolean).join('/')
				let parts = [item.title, item.source, meta].filter(Boolean)
				if (item.reason) parts.push(item.reason)
				log(`[fail] #${item.id}`, parts.join(' | '))
			}
			if (limit > 0 && failures.length > limit) {
				log(`... ${failures.length - limit} more`)
			}
		}
		if (backfilled) log('backfilled metadata for', backfilled, 'rows')
		if (backfilledGn) log('backfilled google news links for', backfilledGn, 'rows')

		finalyze()
		log('\n', stats)
		return stats
	} catch (error) {
		if (error?.code === 'BROWSER_CLOSED') {
			log('[fatal] browser window closed; stopping summarize')
		}
		throw error
	} finally {
		await resumeAutoSave({ flush: false })
	}
}

if (process.argv[1].endsWith('summarize')) {
	;(async () => {
		let stats = await summarize()
		if (!stats) return
		if (!coffeeTodayFolderId) {
			log('Skipping spreadsheet copy (coffeeTodayFolderId not set)')
			return
		}
		try {
			log(`[info] copy spreadsheet: ${spreadsheetId} -> ${coffeeTodayFolderId}`)
			await copyFile(spreadsheetId, coffeeTodayFolderId, 'news-today')
			log('Spreadsheet copied successfully')
		} catch (e) {
			let status = e?.status || e?.code
			let reason = e?.errors?.[0]?.reason
			let message = e?.errors?.[0]?.message || e?.message || ''
			if (status === 404 || reason === 'notFound') {
				log('[warn] copy spreadsheet skipped: folder not found or no access')
				return
			}
			let suffix = [
				status ? `status=${status}` : '',
				reason ? `reason=${reason}` : '',
				message ? `msg=${message}` : '',
			].filter(Boolean).join(' ')
			log(`[warn] copy spreadsheet failed${suffix ? ` (${suffix})` : ''}`)
		}
	})()
}
