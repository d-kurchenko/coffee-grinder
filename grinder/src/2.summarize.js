import fs from 'fs'
import path from 'path'
import { JSDOM, VirtualConsole } from 'jsdom'

import { log } from './log.js'
import logUpdate from 'log-update'
import cliTruncate from 'cli-truncate'
import { sleep } from './sleep.js'
import { news, pauseAutoSave, resumeAutoSave, saveRowByIndex, spreadsheetId, spreadsheetMode } from './store.js'
import { topics, topicsMap } from '../config/topics.js'
import { decodeGoogleNewsUrl, getGoogleNewsDecodeCooldownMs } from './google-news.js'
import { fetchArticle, getLastFetchStatus } from './fetch-article.js'
import { htmlToText } from './html-to-text.js'
import { extractMetaFromHtml } from './meta-extract.js'
import { ai } from './ai.js'
import { browseArticle, finalyze } from './browse-article.js'
import { verifyArticle } from './verify-article.js'
import { buildVerifyContext } from './verify-context.js'
import { searchExternal, sourceFromUrl } from './external-search.js'
import { copyFile } from './google-drive.js'
import { classifyHtmlState } from './services/playwright.js'
import { coffeeTodayFolderId, newsSheet } from '../config/google-drive.js'
import {
	verifyMode,
	verifyMinConfidence,
	verifyShortThreshold,
	verifyFailOpen,
	verifyModel,
	verifyUseSearch,
} from '../config/verification.js'
import { summarizeConfig } from '../config/summarize.js'
import { externalSearch } from '../config/external-search.js'
import {
	getAlternativeArticles,
	classifyAlternativeCandidates,
	isRuntimeArticles,
} from './summarize/articles.js'
import {
	backfillMetaFromDisk,
	backfillTextFromDisk,
	probeCache,
	readHtmlFromDisk,
	saveArticle,
	writeTextCache,
} from './summarize/disk.js'
import {
	backfillGnUrl,
	buildFallbackSearchQueries,
	hydrateFromGoogleNews,
} from './summarize/gn.js'
import { logEvent } from './summarize/logging.js'
import { logging } from '../config/logging.js'
import {
	isBlank,
	isGoogleNewsUrl,
	missingFields,
	normalizeUrl,
	titleFor,
} from './summarize/utils.js'

const minTextLength = 400
const maxHtmlToTextChars = 4_000_000
const fetchAttempts = 2
const verifyStatusColumn = 'verifyStatus'
const contentMethodColumn = 'contentMethod'
const metaTitleColumn = 'metaTitle'
const metaDescriptionColumn = 'metaDescription'
const metaKeywordsColumn = 'metaKeywords'
const metaDateColumn = 'metaDate'
const metaCanonicalUrlColumn = 'metaCanonicalUrl'
const metaImageColumn = 'metaImage'
const metaAuthorColumn = 'metaAuthor'
const metaSiteNameColumn = 'metaSiteName'
const metaSectionColumn = 'metaSection'
const metaTagsColumn = 'metaTags'
const metaLangColumn = 'metaLang'
const progressStepsOverall = ['content', 'verify', 'summarize', 'write']
const progressStepsSub = ['cache', 'fetch', 'jina', 'playwright']
const progressSteps = [...progressStepsOverall, ...progressStepsSub]
const progressBarWidth = 10
let progressTracker = null
const jsdomVirtualConsole = new VirtualConsole()
jsdomVirtualConsole.on('jsdomError', () => {})
jsdomVirtualConsole.on('error', () => {})
jsdomVirtualConsole.on('warn', () => {})

function setupLogTee() {
	const logFile = process.env.LOG_TEE_FILE || process.env.SUMMARIZE_LOG_FILE || ''
	if (!logFile) return null
	let stream = null
	try {
		fs.mkdirSync(path.dirname(logFile), { recursive: true })
		let flags = process.env.LOG_TEE_APPEND === '1' ? 'a' : 'w'
		stream = fs.createWriteStream(logFile, { flags })
	} catch (error) {
		console.error('[warn] log tee init failed:', error?.message || error)
		return null
	}
	const stripAnsi = process.env.LOG_TEE_STRIP_ANSI !== '0'
	const ansiRegex = /\u001b\[[0-9;?]*[ -/]*[@-~]/g
	const ansiTest = /\u001b\[[0-9;?]*[ -/]*[@-~]/
	const writeToFile = (chunk, encoding) => {
		try {
			if (!stream || stream.destroyed) return
			if (stripAnsi) {
				let text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)
				let hadAnsi = ansiTest.test(text)
				text = text.replace(ansiRegex, '').replace(/\r/g, '\n')
				if (hadAnsi && text && !text.endsWith('\n')) text += '\n'
				stream.write(text, 'utf8')
				return
			}
			if (typeof encoding === 'string') {
				stream.write(chunk, encoding)
			} else {
				stream.write(chunk)
			}
		} catch {}
	}
	const wrap = target => {
		const original = target.write.bind(target)
		target.write = (chunk, encoding, cb) => {
			writeToFile(chunk, encoding)
			return original(chunk, encoding, cb)
		}
		return () => {
			target.write = original
		}
	}
	const restoreStdout = wrap(process.stdout)
	const restoreStderr = wrap(process.stderr)
	const cleanup = () => {
		restoreStdout()
		restoreStderr()
		try {
			stream.end()
		} catch {}
	}
	process.on('exit', cleanup)
	process.on('SIGINT', () => {
		cleanup()
		process.exit(130)
	})
	process.on('SIGTERM', () => {
		cleanup()
		process.exit(143)
	})
	return { cleanup }
}

const isSummarizeCli = Array.isArray(process.argv)
	? process.argv.some(arg => String(arg).includes('2.summarize'))
	: false
if (isSummarizeCli) {
	globalThis.__LOG_SUPPRESS_ALL = true
}
setupLogTee()

let cacheUrlAliases = null

function createProgressTracker(total) {
	const state = new Map()
	const order = []
	const spinnerFrames = ['-', '\\', '|', '/']
	let spinnerIndex = 0
	let spinnerTimer = null
	const canUpdate = Boolean(process.stdout.isTTY)
	let headerLines = []
	let footerLines = []
	let headerPrinted = false
	let footerPrinted = false
	let lastOutput = ''
	const terminalStatuses = new Set([
		'ok',
		'skipped',
		'miss',
		'mismatch',
		'reject',
		'captcha',
		'timeout',
		'rate_limit',
		'504',
		'fail',
		'error',
		'no_text',
		'unverified',
	])
	const buildLinkLabel = url => {
		if (!url) return ''
		try {
			let parsed = new URL(url)
			let host = parsed.host.replace(/^www\./, '')
			let path = parsed.pathname ? parsed.pathname.replace(/\/+$/, '') : ''
			let segments = path.split('/').filter(Boolean)
			if (segments.length) {
				return `${host}/${segments.slice(-1)[0]}`
			}
			return host
		} catch {
			return url
		}
	}
	const formatOsc8 = (label, url) => {
		return label || ''
	}
	const sanitizeSummary = text => {
		if (!text) return ''
		return String(text).replace(/\s+/g, ' ').trim()
	}
	const buildContextKey = (url, isFallback, kind) => {
		let key = url || ''
		let bucket = kind || 'net'
		return `${bucket}:${isFallback ? 'alt' : 'orig'}:${key}`
	}
	const formatLinkLine = event => {
		let url = event._originalUrl || event.url || event.gnUrl || ''
		let label = truncate(buildLinkLabel(url) || url, 90)
		let linkText = formatOsc8(label, url)
		let current = getState(event.id)
		let winnerId = current.winnerContextId
		let winnerContext = winnerId ? current.contextMap.get(winnerId) : null
		let winnerIndex = winnerContext ? current.contexts.indexOf(winnerContext) + 1 : 0
		let totalMs = current.eventTotalMs || winnerContext?.total?.ms || 0
		let winnerTime = totalMs ? formatDuration(totalMs) : ''
		let winnerTag = winnerIndex && winnerTime ? ` #${event.id}.${winnerIndex}(${winnerTime})` : ''
		let note = current.note ? ` [${current.note}]` : ''
		return `#${event.id} ${linkText}${winnerTag}${note}`.trimEnd()
	}
	const formatSummaryLine = summary => {
		let summaryText = truncate(sanitizeSummary(summary), 160)
		let value = summaryText || '--'
		return `    summary: ${value}`.trimEnd()
	}
const formatContextLine = (context, index) => {
		let label = truncate(buildLinkLabel(context.url) || context.url || '', 90)
		let linkText = formatOsc8(label, context.url || '')
		let prefix = context.kind === 'cache' ? 'cache' : (context.isFallback ? 'alt' : 'link')
		let origin = context.origin ? `origin:${context.origin}` : ''
		let contentStatus = formatStatusLabel(context.content?.status)
		let contentMethod = context.content?.method ? `(${context.content.method})` : ''
		let contentMs = formatDuration(context.content?.ms)
		let contentTime = contentMs ? `(${contentMs})` : ''
		let prepareStatus = formatStatusLabel(context.prepare?.status)
		let prepareMs = formatDuration(context.prepare?.ms)
		let prepareTime = prepareMs ? `(${prepareMs})` : ''
		let prepareLabel = `prepare:${prepareStatus}${prepareTime}`
		let verifyStatus = formatStatusLabel(context.verify?.status)
		let verifyMs = formatDuration(context.verify?.ms)
		let verifyTime = verifyMs ? `(${verifyMs})` : ''
		let verifyModel = context.verify?.model ? `(${context.verify.model})` : ''
		let verifyNoteValue = context.verify?.note ? String(context.verify.note) : ''
		let verifyNoteLabel = verifyNoteValue ? ` ${verifyNoteValue}` : ''
		if (verifyStatus === '--' && verifyNoteValue) verifyStatus = 'unknown'
		let meta = `${origin ? `${origin} ` : ''}content:${contentStatus}${contentMethod}${contentTime}\t${prepareLabel}\tverify:${verifyStatus}${verifyModel}${verifyTime}${verifyNoteLabel}`
		let indexLabel = index ? `${prefix}${index}` : prefix
		return `    ${indexLabel} ${linkText}  ${meta}`.trimEnd()
	}
	const buildBar = done => {
		let filled = Math.min(progressBarWidth, Math.round((done / totalSteps) * progressBarWidth))
		let empty = Math.max(0, progressBarWidth - filled)
		return `[${'#'.repeat(filled)}${'-'.repeat(empty)}]`
	}
	const shortStatus = value => {
		let text = String(value || '').toLowerCase()
		if (!text) return '--'
		let map = {
			start: 'start',
			ready: 'ready',
			loaded: 'ready',
			wait: 'wait',
			ok: 'ok',
			skipped: 'skipped',
			skip: 'skipped',
			miss: 'miss',
			reject: 'reject',
			mismatch: 'mismatch',
			captcha: 'captcha',
			timeout: 'timeout',
			rate_limit: 'rate_limit',
			'504': '504',
			empty: 'empty',
			fail: 'fail',
			error: 'error',
			no_text: 'no_text',
			mismatch: 'mismatch',
			unverified: 'unverified',
		}
		return map[text] || text
	}
	const formatStatusLabel = value => {
		let text = String(value || '').toLowerCase()
		if (text === 'start') return `${spinnerFrames[spinnerIndex]}`
		if (text === 'wait') return `wait${spinnerFrames[spinnerIndex]}`
		return shortStatus(text)
	}
	const totalSteps = progressStepsOverall.length
	const barFromFraction = fraction => {
		let clamped = Math.max(0, Math.min(1, Number.isFinite(fraction) ? fraction : 0))
		let filled = Math.min(progressBarWidth, Math.round(clamped * progressBarWidth))
		let empty = Math.max(0, progressBarWidth - filled)
		return `[${'#'.repeat(filled)}${'-'.repeat(empty)}]`
	}
	const barFromStatus = status => {
		let text = String(status || '').toLowerCase()
		if (text === 'start') return barFromFraction(0.1)
		if (text === 'wait') return barFromFraction(0.1)
		if (['ok', 'skipped', 'miss', 'mismatch', 'reject', 'captcha', 'timeout', 'rate_limit', '504', 'fail', 'error', 'no_text'].includes(text)) {
			return barFromFraction(1)
		}
		return barFromFraction(0.3)
	}
	const parseProgress = note => {
		if (!note) return null
		let match = String(note).match(/(\d+)\s*\/\s*(\d+)/)
		if (!match) return null
		let current = Number(match[1])
		let totalValue = Number(match[2])
		if (!Number.isFinite(current) || !Number.isFinite(totalValue) || totalValue <= 0) return null
		return current / totalValue
	}
	const formatLine = (indent, label, bar, status, note) => {
		let stepLabel = label.padEnd(10, ' ')
		let statusLabel = String(formatStatusLabel(status)).padEnd(8, ' ')
		let suffix = note ? ` ${note}` : ''
		return `${indent}${stepLabel} ${bar} ${statusLabel}${suffix}`.trimEnd()
	}
	const formatDuration = ms => {
		if (!Number.isFinite(ms) || ms <= 0) return ''
		if (ms < 1000) return `${Math.round(ms)}ms`
		return `${(ms / 1000).toFixed(1)}s`
	}
	const markTiming = (bucket, step, status) => {
		if (!bucket[step]) bucket[step] = { start: 0, end: 0, ms: 0 }
		let now = Date.now()
		if (status === 'start') {
			if (!bucket[step].start) bucket[step].start = now
			return
		}
		if (terminalStatuses.has(String(status || '').toLowerCase())) {
			if (bucket[step].ms && bucket[step].ms > 0) return
			if (!bucket[step].start) bucket[step].start = now
			bucket[step].end = now
			bucket[step].ms = Math.max(0, bucket[step].end - bucket[step].start)
		}
	}
	const formatOverall = (event, doneCount, note) => {
		let current = getState(event.id)
		let bar = buildBar(doneCount)
		let s = formatStatusLabel(current.overall.summarize)
		let w = formatStatusLabel(current.overall.write)
		let sMs = formatDuration(current.timings.overall.summarize?.ms)
		let wMs = formatDuration(current.timings.overall.write?.ms)
		let summarizeModel = current.overallNote.summarize ? `(${current.overallNote.summarize})` : ''
		let status = `summarize:${s}${summarizeModel}${sMs ? `(${sMs})` : ''}\twrite:${w}${wMs ? `(${wMs})` : ''}`
		return formatLine('    ', 'overall', bar, status, '')
	}
	const getState = eventId => {
		let existing = state.get(eventId)
		if (existing) return existing
		let created = {
			done: new Set(),
			contexts: [],
			contextMap: new Map(),
			activeContextId: '',
			primaryContextId: '',
			winnerContextId: '',
			eventStartMs: 0,
			eventTotalMs: 0,
			overall: { content: '', verify: '', summarize: '', write: '' },
			overallNote: { content: '', verify: '', summarize: '', write: '' },
			summary: '',
			summaryLogged: false,
			note: '',
			timings: {
				overall: { content: {}, verify: {}, summarize: {}, write: {} },
			},
		}
		state.set(eventId, created)
		return created
	}
	const ensureContext = (current, { url, isFallback, kind, origin }) => {
		let key = buildContextKey(url, isFallback, kind)
		let existing = current.contextMap.get(key)
		if (existing) {
			if (origin && !existing.origin) existing.origin = origin
			return existing
		}
		let context = {
			id: key,
			url: url || '',
			isFallback: Boolean(isFallback),
			kind: kind || 'net',
			origin: origin || '',
			total: { start: 0, end: 0, ms: 0 },
			content: { status: '--', method: '', ms: 0 },
			verify: { status: '--', ms: 0 },
			prepare: { status: '--', ms: 0 },
			steps: {
				cache: { status: '--', note: '' },
				fetch: { status: '--', note: '' },
				jina: { status: '--', note: '' },
				playwright: { status: '--', note: '' },
			},
			timings: { fetch: {}, jina: {}, playwright: {} },
		}
		current.contextMap.set(key, context)
		current.contexts.push(context)
		return context
	}
	const getActiveContext = current => {
		if (current.activeContextId && current.contextMap.has(current.activeContextId)) {
			return current.contextMap.get(current.activeContextId)
		}
		if (current.contexts.length) return current.contexts[0]
		let context = ensureContext(current, { url: '', isFallback: false })
		current.activeContextId = context.id
		return context
	}
	const render = () => {
		let bodyLines = []
		let hasActive = false
		for (let eventId of order) {
			let current = state.get(eventId)
			if (!current) continue
			let event = current.event
			let summarizeStatus = String(current.overall.summarize || '').toLowerCase()
			let writeStatus = String(current.overall.write || '').toLowerCase()
			if (summarizeStatus === 'start' || summarizeStatus === 'wait' || writeStatus === 'start' || writeStatus === 'wait') {
				hasActive = true
			}
			bodyLines.push(formatLinkLine(event))
			for (let contextIndex = 0; contextIndex < current.contexts.length; contextIndex++) {
				let context = current.contexts[contextIndex]
				let contentStatus = String(context.content?.status || '').toLowerCase()
				let verifyStatus = String(context.verify?.status || '').toLowerCase()
				if (contentStatus === 'start' || contentStatus === 'wait' || verifyStatus === 'start' || verifyStatus === 'wait') {
					hasActive = true
				}
				bodyLines.push(formatContextLine(context, contextIndex + 1))
				if (context.kind !== 'cache') {
					for (let step of progressStepsSub) {
						let entry = context.steps[step] || { status: '--', note: '' }
						let stepStatus = String(entry.status || '').toLowerCase()
						if (stepStatus === 'start' || stepStatus === 'wait') hasActive = true
						let fraction = parseProgress(entry.note)
						let bar = fraction !== null ? barFromFraction(fraction) : barFromStatus(entry.status)
						let duration = formatDuration(context.timings[step]?.ms)
						let note = entry.note
						if (duration) note = note ? `${note} ${duration}` : duration
						bodyLines.push(formatLine('        ', step, bar, entry.status, note))
					}
				}
			}
			bodyLines.push(formatOverall(event, current.done.size, current.note))
			bodyLines.push(formatSummaryLine(current.summary))
		}
		let maxRows = process.stdout.rows || 0
		if (!maxRows || maxRows < 10) maxRows = 40
		if (maxRows > 0) {
			let headerCount = headerLines.length ? headerLines.length + 1 : 0
			let footerCount = footerLines.length ? footerLines.length + 1 : 0
			let available = maxRows - headerCount - footerCount
			if (available > 0 && bodyLines.length > available) {
				bodyLines = bodyLines.slice(bodyLines.length - available)
				if (available >= 2) bodyLines[0] = '...'
			}
		}
		let lines = []
		lines.push(...bodyLines)
		if (footerLines.length) {
			lines.push('')
			for (let line of footerLines) lines.push(line)
		}
		if (canUpdate && hasActive && !spinnerTimer) {
			spinnerTimer = setInterval(() => {
				spinnerIndex = (spinnerIndex + 1) % spinnerFrames.length
				render()
			}, 120)
		} else if ((!hasActive || !canUpdate) && spinnerTimer) {
			clearInterval(spinnerTimer)
			spinnerTimer = null
		}
		if (!canUpdate) {
			if (!headerPrinted && headerLines.length) {
				console.log(headerLines.join('\n'))
				console.log('')
				headerPrinted = true
			}
			let output = lines.join('\n')
			if (output && output !== lastOutput) {
				console.log(output)
				lastOutput = output
			}
			return
		}
		let width = process.stdout.columns || 120
		let output = lines.map(line => cliTruncate(line, width, { position: 'end' })).join('\n')
		logUpdate(output)
	}
	return {
		start(event, index) {
			let current = getState(event.id)
			let summaryText = event?.summary ? truncate(sanitizeSummary(event.summary), 160) : ''
			current.summary = summaryText || ''
			if (summaryText) current.summaryLogged = true
			current.event = event
			current.note = ''
			if (!current.eventStartMs) current.eventStartMs = Date.now()
			order.push(event.id)
			current.overall.content = 'start'
			markTiming(current.timings.overall, 'content', 'start')
			render()
		},
		setContext(event, { url, isFallback, kind, origin }) {
			let current = getState(event.id)
			let context = ensureContext(current, { url, isFallback, kind, origin })
			if (!context.total.start) context.total.start = Date.now()
			current.activeContextId = context.id
			if (!context.content || context.content.status === '--') {
				if (kind === 'cache') {
					context.content = { status: 'ready', method: 'cache', ms: 0 }
				} else {
					context.content = { status: 'start', method: 'lookup', ms: 0 }
				}
			}
			render()
		},
		setWinnerContext(event, { url, isFallback, kind, origin }) {
			let current = getState(event.id)
			let context = ensureContext(current, { url, isFallback, kind, origin })
			current.winnerContextId = context.id
			render()
		},
		setContextContent(event, { status, method, ms }) {
			let current = getState(event.id)
			let context = getActiveContext(current)
			context.content = {
				status: status || context.content?.status || '--',
				method: method || context.content?.method || '',
				ms: Number.isFinite(ms) ? ms : (context.content?.ms || 0),
			}
			render()
		},
		setContextVerify(event, { status, ms, note, model }) {
			let current = getState(event.id)
			let context = getActiveContext(current)
			if (context.total.start && !context.total.end && status !== 'start' && status !== 'wait') {
				context.total.end = Date.now()
				context.total.ms = Math.max(0, context.total.end - context.total.start)
			}
			context.verify = {
				status: status || context.verify?.status || '--',
				ms: Number.isFinite(ms) ? ms : (context.verify?.ms || 0),
				note: note || context.verify?.note || '',
				model: model || context.verify?.model || '',
			}
			render()
		},
		setContextPrepare(event, { status, ms }) {
			let current = getState(event.id)
			let context = getActiveContext(current)
			context.prepare = {
				status: status || context.prepare?.status || '--',
				ms: Number.isFinite(ms) ? ms : (context.prepare?.ms || 0),
			}
			render()
		},
		setSummarizeModel(event, model) {
			let current = getState(event.id)
			current.overallNote.summarize = model || ''
			render()
		},
		setHeader(lines) {
			headerLines = Array.isArray(lines) ? lines.filter(Boolean) : []
			render()
		},
		setFooter(lines) {
			footerLines = Array.isArray(lines) ? lines.filter(Boolean) : []
			if (canUpdate) render()
		},
		begin(event, step) {
			if (!progressSteps.includes(step)) return
			let current = getState(event.id)
			if (progressStepsOverall.includes(step)) {
				markTiming(current.timings.overall, step, 'start')
			} else if (progressStepsSub.includes(step)) {
				let context = getActiveContext(current)
				markTiming(context.timings, step, 'start')
			}
		},
		step(event, step, status, note) {
			if (!progressSteps.includes(step)) return
			let current = getState(event.id)
			if (step === 'summarize' && status === 'ok' && note) {
				let summaryText = truncate(sanitizeSummary(note), 160)
				if (summaryText && summaryText !== current.summary) {
					current.summary = summaryText
					current.summaryLogged = true
				}
			}
			if (progressStepsOverall.includes(step)) {
				let noteChanged = note && note !== current.overallNote[step]
				let isTerminal = status && status !== 'start' && status !== 'wait'
				if (isTerminal && current.done.has(step) && !noteChanged) return
				if (isTerminal) current.done.add(step)
				if (note && step === 'content') current.overallNote.content = note
				markTiming(current.timings.overall, step, status || 'start')
				if (step === 'write' && isTerminal && current.eventStartMs && !current.eventTotalMs) {
					current.eventTotalMs = Math.max(0, Date.now() - current.eventStartMs)
				}
			}
			if (progressStepsSub.includes(step)) {
				let context = getActiveContext(current)
				context.steps[step] = { status: status || 'start', note: note || '' }
				markTiming(context.timings, step, status || 'start')
			} else if (progressStepsOverall.includes(step)) {
				current.overall[step] = status || 'start'
			}
			render()
		},
		setDuration(event, step, ms) {
			let current = getState(event.id)
			if (progressStepsOverall.includes(step)) {
				current.timings.overall[step] = { start: 0, end: 0, ms }
			} else if (progressStepsSub.includes(step)) {
				let context = getActiveContext(current)
				context.timings[step] = { start: 0, end: 0, ms }
			}
			render()
		},
		getDuration(event, step) {
			let current = getState(event.id)
			if (progressStepsOverall.includes(step)) {
				return current.timings.overall[step]?.ms ?? null
			}
			if (progressStepsSub.includes(step)) {
				let context = getActiveContext(current)
				return context.timings[step]?.ms ?? null
			}
			return null
		},
		flushSubsteps() {
			render()
		},
		done() {
			if (spinnerTimer) {
				clearInterval(spinnerTimer)
				spinnerTimer = null
			}
			if (!canUpdate) {
				if (!footerPrinted && footerLines.length) {
					console.log('')
					console.log(footerLines.join('\n'))
					footerPrinted = true
				}
				return
			}
			logUpdate.done()
		},
	}
}

function shouldVerify({ isFallback, textLength }) {
	if (verifyMode === 'always') return true
	if (verifyMode === 'fallback') return isFallback
	if (verifyMode === 'short') return textLength < verifyShortThreshold
	return false
}

function cloneEvent(event) {
	let copy = { ...event }
	if (isRuntimeArticles(event) && Array.isArray(event?._articles)) {
		copy._articles = event._articles.map(item => ({ ...item }))
		copy._articlesOrigin = event._articlesOrigin
	}
	return copy
}

function commitEvent(target, source) {
	for (let [key, value] of Object.entries(source || {})) {
		if (key === 'articles' || key === '_articles' || key === '_articlesOrigin') continue
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

function applyFallbackSelection(event, alt, altUrl) {
	if (isBlank(event.source) && alt?.source) event.source = alt.source
	if (isBlank(event.gnUrl) && !isBlank(alt?.gnUrl)) event.gnUrl = alt.gnUrl
	if (isBlank(event.titleEn) && !isBlank(alt?.titleEn)) event.titleEn = alt.titleEn
	if (isBlank(event.url) && altUrl) event.url = altUrl
	if (isBlank(event.gnUrl) && isBlank(event.alternativeUrl) && !isBlank(altUrl)) {
		let originalUrl = normalizeUrl(event.url || '')
		if (!originalUrl || originalUrl !== altUrl) {
			event.alternativeUrl = altUrl
		}
	}
	setOriginalUrlIfMissing(event)
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
	const resolveContentDuration = methodLabel => {
		if (!progressTracker?.getDuration) return null
		if (!methodLabel) return null
		let normalized = String(methodLabel).toLowerCase()
		if (normalized === 'cache') return 0
		if (normalized === 'fetch') return progressTracker.getDuration(event, 'fetch')
		if (normalized === 'jina') return progressTracker.getDuration(event, 'jina')
		if (normalized === 'browse' || normalized === 'playwright') return progressTracker.getDuration(event, 'playwright')
		return null
	}
	if (event._contentUrl) {
		let methodLabel = event._contentMethod || method || ''
		let durationMs = resolveContentDuration(methodLabel)
		progressTracker?.setContext?.(event, { url: event._contentUrl, isFallback: event._contentIsFallback, kind: 'net' })
		progressTracker?.setWinnerContext?.(event, { url: event._contentUrl, isFallback: event._contentIsFallback, kind: 'net' })
		progressTracker?.setContextContent?.(event, { status: 'ok', method: methodLabel, ms: durationMs })
		if (Number.isFinite(durationMs)) progressTracker?.setDuration?.(event, 'content', durationMs)
		progressTracker?.step(event, 'content', 'ok', methodLabel)
		progressTracker?.flushSubsteps?.(event)
		return
	}
	event._contentUrl = url || ''
	event._contentSource = source || ''
	event._contentMethod = method || ''
	event._contentIsFallback = Boolean(isFallback)
	event[contentMethodColumn] = event._contentMethod
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
	let durationMs = resolveContentDuration(event._contentMethod || '')
	progressTracker?.setContext?.(event, { url: event._contentUrl, isFallback: event._contentIsFallback, kind: 'net' })
	progressTracker?.setWinnerContext?.(event, { url: event._contentUrl, isFallback: event._contentIsFallback, kind: 'net' })
	progressTracker?.setContextContent?.(event, { status: 'ok', method: event._contentMethod, ms: durationMs })
	if (Number.isFinite(durationMs)) progressTracker?.setDuration?.(event, 'content', durationMs)
	progressTracker?.flushSubsteps?.(event)
	progressTracker?.step(event, 'content', 'ok', event._contentMethod)
}

function applyContentMeta(event, meta, method) {
	if (!meta || typeof meta !== 'object') return
	if (!event._contentMeta) event._contentMeta = {}
	let target = event._contentMeta
	for (let [key, value] of Object.entries(meta)) {
		if (!value) continue
		if (!target[key]) target[key] = value
	}
	if (!event._contentMetaMethod && method) event._contentMetaMethod = method
	if (isBlank(event.titleEn) && meta.title) event.titleEn = meta.title
	if (isBlank(event.date) && meta.date) event.date = meta.date
	if (isBlank(event.url) && meta.canonicalUrl) event.url = meta.canonicalUrl
	if (isBlank(event.description) && meta.description) event.description = meta.description
	if (isBlank(event.keywords) && meta.keywords) event.keywords = meta.keywords

	if (isBlank(event[metaTitleColumn]) && meta.title) event[metaTitleColumn] = meta.title
	if (isBlank(event[metaDescriptionColumn]) && meta.description) event[metaDescriptionColumn] = meta.description
	if (isBlank(event[metaKeywordsColumn]) && meta.keywords) event[metaKeywordsColumn] = meta.keywords
	if (isBlank(event[metaDateColumn]) && (meta.publishedTime || meta.date)) event[metaDateColumn] = meta.publishedTime || meta.date
	if (isBlank(event[metaCanonicalUrlColumn]) && meta.canonicalUrl) event[metaCanonicalUrlColumn] = meta.canonicalUrl
	if (isBlank(event[metaImageColumn]) && meta.image) event[metaImageColumn] = meta.image
	if (isBlank(event[metaAuthorColumn]) && meta.author) event[metaAuthorColumn] = meta.author
	if (isBlank(event[metaSiteNameColumn]) && meta.siteName) event[metaSiteNameColumn] = meta.siteName
	if (isBlank(event[metaSectionColumn]) && meta.section) event[metaSectionColumn] = meta.section
	if (isBlank(event[metaTagsColumn]) && meta.tags) event[metaTagsColumn] = meta.tags
	if (isBlank(event[metaLangColumn]) && (meta.lang || meta.locale)) event[metaLangColumn] = meta.lang || meta.locale
}

function truncate(text, max = 220) {
	if (!text) return ''
	if (text.length <= max) return text
	return text.slice(0, max - 3) + '...'
}

function summarizeSearchResults(results, limit = 8) {
	if (!Array.isArray(results) || !results.length) return []
	return results.slice(0, limit).map(item => ({
		source: item.source || '',
		title: truncate(String(item.titleEn || item.titleRu || '').replace(/\s+/g, ' '), 140),
		url: item.url || '',
		gnUrl: item.gnUrl || '',
		origin: item.origin || item.provider || item.from || '',
		level: item.level,
	}))
}

function logSearchQuery(event, { phase, provider, query, queries }) {
	logEvent(event, {
		phase,
		status: 'query',
		provider,
		query,
		queries,
	}, `#${event.id} ${phase} query`, 'info')
}

function logSearchResults(event, { phase, provider, query, results }) {
	let count = Array.isArray(results) ? results.length : 0
	logEvent(event, {
		phase,
		status: count ? 'ok' : 'empty',
		provider,
		query,
		count,
		results: summarizeSearchResults(results),
	}, `#${event.id} ${phase} ${count}`, count ? 'info' : 'warn')
}

function logCandidateDecision(event, candidate, status, reason, { phase = 'fallback_candidate', provider = '', query = '' } = {}) {
	let level = Number.isFinite(candidate?.level) ? candidate.level : undefined
	let logLevel = (status === 'accepted' || status === 'attempt' || status === 'selected') ? 'info' : 'warn'
	logEvent(event, {
		phase,
		status,
		reason,
		provider,
		query,
		candidateSource: candidate?.source || '',
		candidateUrl: candidate?.url || '',
		candidateGnUrl: candidate?.gnUrl || '',
		candidateTitle: candidate?.titleEn || candidate?.titleRu || '',
		candidateOrigin: candidate?.origin || candidate?.provider || candidate?.from || '',
		candidateLevel: level,
	}, `#${event.id} ${phase} ${status} ${candidate?.source || ''}${reason ? ` (${reason})` : ''}`, logLevel)
}

function logCacheLine(event, status, info = {}) {
	if (progressTracker) return
	let prev = globalThis.__LOG_SUPPRESS
	globalThis.__LOG_SUPPRESS = false
	let parts = [`[cache] #${event?.id || ''}`, status]
	if (info.reason) parts.push(`reason=${info.reason}`)
	if (info.key) parts.push(`key=${String(info.key).slice(0, 10)}`)
	if (info.url) parts.push(`url=${truncate(info.url, 90)}`)
	if (info.hasHtml || info.hasTxt) {
		parts.push(`files=${info.hasHtml ? 1 : 0}/${info.hasTxt ? 1 : 0}`)
	}
	log(parts.join(' ').trim())
	globalThis.__LOG_SUPPRESS = prev
}

function buildHostSlug(url) {
	if (!url) return ''
	try {
		let candidate = url
		if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(candidate)) {
			candidate = `https://${candidate}`
		}
		let parsed = new URL(candidate)
		let host = parsed.hostname.replace(/^www\./, '')
		let pathValue = parsed.pathname ? parsed.pathname.replace(/\/+$/, '') : ''
		let segments = pathValue.split('/').filter(Boolean)
		let slug = segments.length ? segments[segments.length - 1] : ''
		if (!slug) return ''
		return `${host}/${slug}`
	} catch {
		return ''
	}
}

function buildCacheAliasIndex() {
	let map = new Map()
	let dir = 'articles'
	try {
		if (!fs.existsSync(dir)) return map
		let entries = fs.readdirSync(dir)
		for (let name of entries) {
			if (!name.endsWith('.html')) continue
			let filePath = path.join(dir, name)
			let fd = null
			try {
				fd = fs.openSync(filePath, 'r')
				let buffer = Buffer.alloc(2048)
				let bytes = fs.readSync(fd, buffer, 0, buffer.length, 0)
				if (!bytes) continue
				let text = buffer.toString('utf8', 0, bytes)
				let match = text.match(/^<!--\s*([\s\S]*?)\s*-->/)
				let url = match?.[1] ? normalizeUrl(match[1].trim()) : ''
				if (!url) continue
				let key = buildHostSlug(url)
				if (!key || map.has(key)) continue
				map.set(key, url)
			} catch {
				continue
			} finally {
				if (fd) fs.closeSync(fd)
			}
		}
	} catch {
		return map
	}
	return map
}

function resolveCacheAlias(url) {
	if (!cacheUrlAliases || !url) return ''
	let key = buildHostSlug(url)
	if (!key) return ''
	return cacheUrlAliases.get(key) || ''
}

async function tryCache(event, url, { isFallback = false, origin = '', last, contentSource = '' } = {}) {
	progressTracker?.setContext?.(event, { url, isFallback, kind: 'net', origin })
	progressTracker?.step(event, 'cache', 'start')
	let cacheUrl = url
	let aliasUrl = resolveCacheAlias(url)
	let aliasUsed = false
	if (aliasUrl && aliasUrl !== url) {
		cacheUrl = aliasUrl
		aliasUsed = true
	}
	let cacheProbe = probeCache(event, cacheUrl)
	if (!cacheProbe.available) {
		if (cacheProbe.reason === 'missing') {
			logEvent(event, {
				phase: 'cache',
				status: 'miss',
				reason: 'no_files',
				cacheKey: cacheProbe.key,
				cacheUrl: cacheProbe.url,
			}, `#${event.id} cache miss (no files)`, 'warn')
			logCacheLine(event, 'miss', { reason: 'no_files', key: cacheProbe.key, url: cacheProbe.url })
			progressTracker?.step(event, 'cache', 'miss', aliasUsed ? 'alias_no_files' : 'no_files')
			return { ok: false, cacheMetaHit: false, cacheTextHit: false, reason: 'no_files' }
		}
		logEvent(event, {
			phase: 'cache',
			status: 'skip',
			reason: cacheProbe.reason,
		}, `#${event.id} cache skip (${cacheProbe.reason})`, 'warn')
		logCacheLine(event, 'skip', { reason: cacheProbe.reason })
		progressTracker?.step(event, 'cache', 'skipped', cacheProbe.reason || '')
		return { ok: false, cacheMetaHit: false, cacheTextHit: false, reason: cacheProbe.reason }
	}
	logEvent(event, {
		phase: 'cache',
		status: 'probe',
		cacheKey: cacheProbe.key,
		cacheUrl: cacheProbe.url,
		hasHtml: cacheProbe.hasHtml,
		hasTxt: cacheProbe.hasTxt,
	}, `#${event.id} cache probe`, 'info')
	logCacheLine(event, 'probe', { key: cacheProbe.key, url: cacheProbe.url, hasHtml: cacheProbe.hasHtml, hasTxt: cacheProbe.hasTxt })

	let cacheMetaHit = backfillMetaFromDisk(event, cacheUrl)
	let cacheTextHit = backfillTextFromDisk(event, cacheUrl)
	let textLength = event.text?.length || 0
	let shortTextMiss = false
	if (cacheTextHit && textLength > 0 && textLength <= minTextLength) {
		cacheTextHit = false
		event.text = ''
	}
	if (!cacheTextHit) {
		let cachedHtml = readHtmlFromDisk(event, cacheUrl)
		if (cachedHtml) {
			let extracted = extractText(cachedHtml)
			if (extracted && extracted.length > minTextLength) {
				event.text = extracted
				cacheTextHit = true
				textLength = extracted.length
				writeTextCache(event, extracted, cacheUrl)
			}
		}
	}
	if (!cacheTextHit && textLength > 0 && textLength <= minTextLength) {
		logEvent(event, {
			phase: 'cache',
			status: 'miss',
			reason: 'short_text',
			cacheUrl: cacheUrl || '',
			cacheKey: cacheProbe?.key,
			textLength,
		}, `#${event.id} cache miss (short text)`, 'warn')
		logCacheLine(event, 'miss', { reason: 'short_text', key: cacheProbe?.key, url: cacheUrl || '' })
		shortTextMiss = true
	}
	if (cacheMetaHit || cacheTextHit) {
		logEvent(event, {
			phase: 'cache',
			status: 'hit',
			cacheMeta: cacheMetaHit,
			cacheText: cacheTextHit,
			cacheUrl: cacheUrl || '',
			cacheKey: cacheProbe?.key,
			cacheAliasUsed: aliasUsed,
		}, `#${event.id} cache hit`, 'info')
		logCacheLine(event, 'hit', { key: cacheProbe?.key, url: cacheUrl || '', hasHtml: cacheProbe?.hasHtml, hasTxt: cacheProbe?.hasTxt })
		if (cacheTextHit) {
			progressTracker?.step(event, 'cache', 'ok', aliasUsed ? 'alias' : '')
		} else {
			let note = shortTextMiss ? 'short_text' : (cacheMetaHit ? 'meta_only' : '')
			if (aliasUsed) note = note ? `alias_${note}` : 'alias_meta'
			progressTracker?.step(event, 'cache', 'miss', note)
		}
	} else if (!shortTextMiss && !isBlank(url) && (!event.text || event.text.length <= minTextLength)) {
		logEvent(event, {
			phase: 'cache',
			status: 'miss',
			cacheUrl: cacheUrl || '',
			cacheKey: cacheProbe?.key,
		}, `#${event.id} cache miss`, 'warn')
		logCacheLine(event, 'miss', { key: cacheProbe?.key, url: cacheUrl || '' })
		progressTracker?.step(event, 'cache', 'miss', aliasUsed ? 'alias_miss' : '')
	}

	if (event.text?.length > minTextLength) {
		progressTracker?.setContext?.(event, { url, isFallback, kind: 'net', origin })
		progressTracker?.setWinnerContext?.(event, { url, isFallback, kind: 'net', origin })
		progressTracker?.setContextContent?.(event, { status: 'ready', method: 'cache', ms: 0 })
		let verify = await verifyText({
			event,
			url,
			text: event.text,
			isFallback,
			method: 'cache',
			attempt: 0,
			last,
			contextKind: 'net',
		})
		if (verify?.ok) {
			let overallStatus = verify?.status === 'skipped' ? 'skipped' : (verify?.status === 'unverified' ? 'unverified' : 'ok')
			if (Number.isFinite(verify?.durationMs)) {
				progressTracker?.setDuration?.(event, 'verify', verify.durationMs)
			}
			progressTracker?.step(event, 'verify', overallStatus, 'cache')
			applyVerifyStatus(event, verify)
			setContentSource(event, {
				url,
				source: contentSource || event._originalSource || event.source || '',
				method: 'cache',
				isFallback,
			})
			return { ok: true, cacheMetaHit, cacheTextHit, verify }
		}
		logEvent(event, {
			phase: 'cache_verify',
			status: 'mismatch',
			reason: verify?.reason,
			pageSummary: verify?.pageSummary,
		}, `#${event.id} cached text mismatch`, 'warn')
		logEvent(event, {
			phase: 'cache',
			status: 'reject',
			reason: 'verify_mismatch',
		}, `#${event.id} cache rejected (verify mismatch)`, 'warn')
		logCacheLine(event, 'reject', { reason: 'verify_mismatch', key: cacheProbe?.key, url: url || '' })
		progressTracker?.step(event, 'cache', 'mismatch')
		progressTracker?.setContextContent?.(event, { status: 'mismatch', method: 'cache', ms: 0 })
		resetTextFields(event)
		return { ok: false, cacheMetaHit, cacheTextHit, mismatch: true }
	}
	return { ok: false, cacheMetaHit, cacheTextHit, reason: 'no_text' }
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

function classifyPageState(html, meta = {}) {
	let title = meta?.title || meta?.metaTitle || ''
	return classifyHtmlState(html || '', title || '')
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

function formatDuration(ms) {
	if (!Number.isFinite(ms) || ms <= 0) return ''
	if (ms < 1000) return `${Math.round(ms)}ms`
	let seconds = ms / 1000
	if (seconds < 60) return `${seconds.toFixed(1)}s`
	let minutes = Math.floor(seconds / 60)
	let remainder = Math.round(seconds % 60)
	return `${minutes}m${String(remainder).padStart(2, '0')}s`
}

function formatCountdown(ms) {
	let totalSeconds = Math.max(0, Math.ceil((ms || 0) / 1000))
	let minutes = Math.floor(totalSeconds / 60)
	let seconds = totalSeconds % 60
	if (minutes > 0) return `${minutes}m${String(seconds).padStart(2, '0')}s`
	return `${seconds}s`
}

async function sleepWithCountdown(totalMs, onTick, intervalMs = 1000) {
	let start = Date.now()
	let remaining = Math.max(0, totalMs || 0)
	if (onTick) onTick(remaining)
	while (remaining > 0) {
		let step = Math.min(intervalMs, remaining)
		await sleep(step)
		remaining = Math.max(0, totalMs - (Date.now() - start))
		if (onTick) onTick(remaining)
	}
}



async function verifyText({ event, url, text, isFallback, method, attempt, last, contextKind = 'net', progress = true }) {
	if (progress) {
		progressTracker?.setContext?.(event, { url, isFallback, kind: contextKind })
		progressTracker?.setContextVerify?.(event, { status: 'start', ms: 0 })
	}
	let waitMs = 0
	let prepareMs = 0
	let aiMs = 0
	if (!shouldVerify({ isFallback, textLength: text.length })) {
		let durationMs = 0
		logEvent(event, {
			phase: 'verify',
			status: 'skipped',
			method,
			attempt,
			textLength: text.length,
		}, `#${event.id} verify skipped (${method})`, 'info')
		if (progress) {
			progressTracker?.setContextPrepare?.(event, { status: 'skipped', ms: 0 })
			progressTracker?.setContextVerify?.(event, { status: 'skipped', ms: durationMs })
		}
		return { ok: true, status: 'skipped', verified: false, skipped: true, durationMs }
	}
	if (isBlank(event._originalUrl) && !isBlank(event.gnUrl)) {
		let decoded = await decodeUrl(event.gnUrl, last)
		if (decoded && isBlank(event.url)) {
			event.url = decoded
		}
		setOriginalUrlIfMissing(event)
	}
	let verifyWait = last.verify.time + last.verify.delay - Date.now()
	if (verifyWait > 0) {
		let waitStart = Date.now()
		await sleepWithCountdown(verifyWait, remaining => {
			if (!progress) return
			let note = `wait:${formatCountdown(remaining)} left`
			progressTracker?.setContextVerify?.(event, { status: 'wait', ms: 0, note })
		})
		waitMs = Math.max(0, Date.now() - waitStart)
	}
	last.verify.time = Date.now()
	if (progress) progressTracker?.setContextPrepare?.(event, { status: 'start', ms: 0 })
	let prepareStart = Date.now()
	let context = await buildVerifyContext(event)
	prepareMs = Date.now() - prepareStart
	if (progress) progressTracker?.setContextPrepare?.(event, { status: 'ok', ms: prepareMs })
	const verifyStarted = Date.now()
	let aiStart = Date.now()
	let result = await verifyArticle({
		original: context,
		candidate: { url, text },
		minConfidence: verifyMinConfidence,
		failOpen: verifyFailOpen,
		debug: logging.includeVerifyPrompt,
		debugMaxChars: logging.verifyPromptMaxChars,
	})
	aiMs = Date.now() - aiStart
	applyVerifyStatus(event, result)
	let verifyDebug = result?.debug
	let modelName = result?.model || verifyDebug?.model || verifyModel || ''
	let useSearch = result?.useSearch ?? verifyDebug?.useSearch ?? verifyUseSearch
	let modelLabel = modelName
	if (modelLabel && useSearch) modelLabel = `${modelLabel}+search`
	if (modelLabel && result?.fallbackUsed) modelLabel = `${modelLabel}+fallback`
	let status = result?.status || (result?.ok ? 'ok' : (result?.error ? 'error' : 'mismatch'))
	let summarySnippet = result?.pageSummary ? ` | ${truncate(result.pageSummary)}` : ''
	let errorMessage = result?.error ? String(result.error?.message || result.error) : undefined
	let statusMessage = status === 'unverified' ? 'unverified (gpt unavailable)' : status
	let durationMs = Date.now() - verifyStarted
	let verifyNote = ''
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
		waitMs,
		contextMs: prepareMs,
		aiMs,
		verifyModel: modelName || verifyDebug?.model,
		verifyTemperature: verifyDebug?.temperature,
		verifyUseSearch: useSearch,
		verifyFallback: result?.fallbackUsed || verifyDebug?.fallbackUsed,
		verifyFallbackMaxChars: verifyDebug?.fallbackMaxChars,
		verifyFallbackContextMaxChars: verifyDebug?.fallbackContextMaxChars,
		verifySystem: verifyDebug?.system,
		verifyPrompt: verifyDebug?.prompt,
	}, `#${event.id} verify ${statusMessage} (${method})${summarySnippet}`, result?.ok ? 'ok' : 'warn')
	let contextStatus = status === 'unverified' ? 'unverified' : (result?.ok ? 'ok' : status)
	if (progress) {
		progressTracker?.setContextVerify?.(event, { status: contextStatus, ms: durationMs, note: verifyNote, model: modelLabel })
	}
	result.durationMs = durationMs
	result.verifyNote = verifyNote
	result.prepareMs = prepareMs
	if (modelLabel) result.modelLabel = modelLabel
	return result
}

async function fetchTextWithRetry(event, url, last, { isFallback = false, origin = '' } = {}) {
	let foundText = false
	let lastPageState = null
	const normalizeFetchStatus = value => {
		if (value === null || value === undefined) return ''
		if (typeof value === 'number') return value
		let text = String(value).trim().toLowerCase()
		if (/^\d+$/.test(text)) return Number(text)
		return text
	}
	const isNonRetryableStatus = status => {
		return status === 429 || status === 403 || status === 503 || status === 'captcha'
	}
	progressTracker?.setContext?.(event, { url, isFallback, kind: 'net', origin })
	let lastFailureStatus = null
	let lastFailureMethod = null
	for (let attempt = 1; attempt <= fetchAttempts; attempt++) {
		let blockedStatus = null
		let mismatchResult = null
		let mismatchHtml = null
		let mismatchText = null
		let browsePromise = null
		let browseStarted = false
		let fetchMethod = ''
		let fetchMeta = null
		let onFetchMethod = method => {
			fetchMethod = method
			if (method === 'fetch' || method === 'jina') lastFailureMethod = method
			if (method === 'fetch') progressTracker?.step(event, 'fetch', 'ok')
			if (method === 'jina') progressTracker?.step(event, 'jina', 'ok')
			if (method === 'captcha') {
				lastFailureStatus = 'captcha'
				lastFailureMethod = 'fetch'
				progressTracker?.step(event, 'fetch', 'captcha')
				let durationMs = progressTracker?.getDuration?.(event, 'fetch')
				progressTracker?.setContextContent?.(event, { status: 'captcha', method: 'fetch', ms: durationMs })
			}
			if (method === 'timeout') {
				lastFailureStatus = 'timeout'
				lastFailureMethod = 'fetch'
				progressTracker?.step(event, 'fetch', 'timeout')
				let durationMs = progressTracker?.getDuration?.(event, 'fetch')
				progressTracker?.setContextContent?.(event, { status: 'timeout', method: 'fetch', ms: durationMs })
			}
		}
		let updateContentFromMethod = methodLabel => {
			if (!methodLabel) return
			let normalized = String(methodLabel).toLowerCase()
			let durationMs = null
			if (normalized === 'cache') durationMs = 0
			else if (normalized === 'fetch') durationMs = progressTracker?.getDuration?.(event, 'fetch')
			else if (normalized === 'jina') durationMs = progressTracker?.getDuration?.(event, 'jina')
			else if (normalized === 'browse' || normalized === 'playwright') durationMs = progressTracker?.getDuration?.(event, 'playwright')
			if (Number.isFinite(durationMs)) progressTracker?.setDuration?.(event, 'content', durationMs)
			progressTracker?.setContextContent?.(event, { status: 'ok', method: methodLabel, ms: durationMs })
			progressTracker?.step(event, 'content', 'ok', methodLabel)
		}
		let startBrowse = async () => {
			browseStarted = true
			let html = ''
			let meta = {}
			let browseFailed = false
			progressTracker?.step(event, 'playwright', 'start')
			try {
				let result = await browseArticle(url, { ignoreCooldown: !isFallback })
				if (result && typeof result === 'object') {
					html = result.html || ''
					meta = result.meta || {}
				} else {
					html = result || ''
				}
			} catch (error) {
				if (error?.code === 'BROWSER_CLOSED') throw error
				if (error?.code === 'CAPTCHA') {
					logEvent(event, {
						phase: 'browse',
						method: 'browse',
						status: 'captcha',
						attempt,
					}, `#${event.id} browse captcha`, 'warn')
					progressTracker?.step(event, 'playwright', 'captcha')
					let durationMs = progressTracker?.getDuration?.(event, 'playwright')
					progressTracker?.setContextContent?.(event, { status: 'captcha', method: 'playwright', ms: durationMs })
					return { html: '', meta: {}, browseFailed: false, aborted: true, abortReason: 'captcha' }
				}
				if (error?.code === 'TIMEOUT') {
					logEvent(event, {
						phase: 'browse',
						method: 'browse',
						status: 'timeout',
						attempt,
					}, `#${event.id} browse timeout`, 'warn')
					progressTracker?.step(event, 'playwright', 'timeout')
					let durationMs = progressTracker?.getDuration?.(event, 'playwright')
					progressTracker?.setContextContent?.(event, { status: 'timeout', method: 'playwright', ms: durationMs })
					return { html: '', meta: {}, browseFailed: true, aborted: true, abortReason: 'timeout' }
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
				return { html: '', meta: {}, browseFailed: true, aborted: true, abortReason: 'error' }
			}
			return { html, meta, browseFailed }
		}
		progressTracker?.step(event, 'fetch', 'start')
		let html = await fetchArticle(url, { onMethod: onFetchMethod })
		if (html) fetchMeta = extractMetaFromHtml(html)
		let lastStatus = getLastFetchStatus(url)
		let text = extractText(html)
		let fetchState = classifyPageState(html, fetchMeta)
		lastPageState = fetchState
		let normalizedStatus = normalizeFetchStatus(lastStatus || lastFailureStatus)
		if (!text && isNonRetryableStatus(normalizedStatus)) {
			let statusLabel = normalizedStatus === 'captcha'
				? 'captcha'
				: (normalizedStatus === 429 || normalizedStatus === 503 ? 'rate_limited' : 'forbidden')
			logEvent(event, {
				phase: 'fetch',
				method: 'fetch',
				status: statusLabel,
				attempt,
				httpStatus: typeof normalizedStatus === 'number' ? normalizedStatus : undefined,
			}, `#${event.id} fetch ${statusLabel} (${normalizedStatus})`, 'warn')
			if (statusLabel === 'rate_limited') progressTracker?.step(event, 'fetch', 'rate_limit')
			else if (statusLabel === 'captcha') progressTracker?.step(event, 'fetch', 'captcha')
			else progressTracker?.step(event, 'fetch', 'error')
			lastFailureStatus = statusLabel
			lastFailureMethod = 'fetch'
			blockedStatus = normalizedStatus
			let durationMs = progressTracker?.getDuration?.(event, 'fetch') || 0
			progressTracker?.setContextContent?.(event, {
				status: statusLabel === 'forbidden' ? 'error' : (statusLabel === 'rate_limited' ? 'rate_limit' : 'captcha'),
				method: 'fetch',
				ms: durationMs,
			})
			progressTracker?.step(event, 'content', statusLabel === 'forbidden' ? 'error' : (statusLabel === 'rate_limited' ? 'rate_limit' : 'captcha'), 'fetch')
		}
		if (!text && lastStatus === 504) {
			progressTracker?.step(event, 'fetch', '504')
			lastFailureStatus = '504'
			lastFailureMethod = 'fetch'
		}
		if (!text && lastStatus === 'timeout') {
			progressTracker?.step(event, 'fetch', 'timeout')
			lastFailureStatus = 'timeout'
			lastFailureMethod = 'fetch'
		}
		if (!text && lastStatus === 'captcha') {
			progressTracker?.step(event, 'fetch', 'captcha')
			lastFailureStatus = 'captcha'
			lastFailureMethod = 'fetch'
		}
		if (!text && !lastFailureStatus) {
			progressTracker?.step(event, 'fetch', 'no_text')
			lastFailureStatus = 'no_text'
			lastFailureMethod = 'fetch'
		}
		if (text) {
			foundText = true
			logEvent(event, {
				phase: 'fetch',
				method: fetchMethod || 'fetch',
				status: 'ok',
				attempt,
				textLength: text.length,
			}, `#${event.id} ${(fetchMethod || 'fetch')} ok (${attempt}/${fetchAttempts})`, 'ok')
			let methodLabel = fetchMethod || 'fetch'
			if (fetchMethod !== 'jina') progressTracker?.step(event, 'jina', 'skipped', 'not used')
			progressTracker?.step(event, 'playwright', 'skipped', 'fetch ok')
			updateContentFromMethod(methodLabel)
			let verify = await verifyText({ event, url, text, isFallback, method: methodLabel, attempt, last, contextKind: 'net' })
			if (verify?.ok) {
				let overallStatus = verify?.status === 'skipped' ? 'skipped' : (verify?.status === 'unverified' ? 'unverified' : 'ok')
				if (Number.isFinite(verify?.durationMs)) {
					progressTracker?.setDuration?.(event, 'verify', verify.durationMs)
				}
				progressTracker?.step(event, 'verify', overallStatus, methodLabel)
				if (fetchMeta && Object.values(fetchMeta).some(Boolean)) {
					applyContentMeta(event, fetchMeta, methodLabel)
				}
				return { ok: true, html, text, verify, method: methodLabel, url }
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
				pageState: fetchState?.state || '',
				pageStateReason: fetchState?.reason || '',
			}, `#${event.id} fetch no text (${attempt}/${fetchAttempts})`, 'warn')
		}

		if (mismatchResult && !summarizeConfig.browseOnMismatch) {
			progressTracker?.step(event, 'playwright', 'skipped', 'mismatch')
			return { ok: false, mismatch: true, verify: mismatchResult, html: mismatchHtml, text: mismatchText }
		}

		if (!browsePromise) browsePromise = startBrowse()
		let browseResult = await browsePromise
		html = browseResult?.html || ''
		let browseMeta = browseResult?.meta || {}
		if (!browseMeta || !Object.values(browseMeta).some(Boolean)) {
			browseMeta = html ? extractMetaFromHtml(html) : {}
		}
		let browseState = classifyPageState(html, browseMeta)
		lastPageState = browseState
		let browseFailed = Boolean(browseResult?.browseFailed)
		text = extractText(html)
		if (!text && browseResult?.aborted && browseResult?.abortReason) {
			lastFailureStatus = browseResult.abortReason
			lastFailureMethod = 'playwright'
		}
		if (text) {
			foundText = true
			logEvent(event, {
				phase: 'fetch',
				method: 'browse',
				status: 'ok',
				attempt,
				textLength: text.length,
			}, `#${event.id} browse ok (${attempt}/${fetchAttempts})`, 'ok')
			progressTracker?.step(event, 'playwright', 'ok')
			updateContentFromMethod('browse')
			let verify = await verifyText({ event, url, text, isFallback, method: 'browse', attempt, last, contextKind: 'net' })
			if (verify?.ok) {
				let overallStatus = verify?.status === 'skipped' ? 'skipped' : (verify?.status === 'unverified' ? 'unverified' : 'ok')
				if (Number.isFinite(verify?.durationMs)) {
					progressTracker?.setDuration?.(event, 'verify', verify.durationMs)
				}
				progressTracker?.step(event, 'verify', overallStatus, 'browse')
				if (browseMeta && Object.values(browseMeta).some(Boolean)) {
					applyContentMeta(event, browseMeta, 'browse')
				}
				return { ok: true, html, text, verify, method: 'browse', url }
			}
			if (verify?.status === 'mismatch') {
				mismatchResult = verify
				mismatchHtml = html
				mismatchText = text
			}
		} else if (!browseFailed && !browseResult?.aborted) {
			logEvent(event, {
				phase: 'fetch',
				method: 'browse',
				status: 'no_text',
				attempt,
				pageState: browseState?.state || '',
				pageStateReason: browseState?.reason || '',
			}, `#${event.id} browse no text (${attempt}/${fetchAttempts})`, 'warn')
			progressTracker?.step(event, 'playwright', 'no_text')
		}

		if (mismatchResult) {
			return { ok: false, mismatch: true, verify: mismatchResult, html: mismatchHtml, text: mismatchText }
		}
		log(`article text missing (${attempt}/${fetchAttempts})`)
		if (blockedStatus) {
			return { ok: false, blocked: true, status: blockedStatus }
		}
	}
	if (!foundText) {
		let finalStatus = lastFailureStatus || 'no_text'
		let finalMethod = lastFailureMethod || 'fetch'
		let durationMs = 0
		if (finalMethod === 'fetch') durationMs = progressTracker?.getDuration?.(event, 'fetch') || 0
		else if (finalMethod === 'jina') durationMs = progressTracker?.getDuration?.(event, 'jina') || 0
		else if (finalMethod === 'playwright') durationMs = progressTracker?.getDuration?.(event, 'playwright') || 0
		progressTracker?.setContextContent?.(event, { status: finalStatus, method: finalMethod, ms: durationMs })
		progressTracker?.step(event, 'content', finalStatus, finalMethod)
		logEvent(event, {
			phase: 'fetch',
			status: 'no_text',
			attempts: fetchAttempts,
			pageState: lastPageState?.state || '',
			pageStateReason: lastPageState?.reason || '',
		}, `#${event.id} no text after ${fetchAttempts} attempts`, 'warn')
	}
}

export async function summarize() {
	const wasSuppressAll = globalThis.__LOG_SUPPRESS_ALL === true
	globalThis.__LOG_SUPPRESS_ALL = true
	pauseAutoSave()
	try {
		let runStart = Date.now()
		globalThis.__LOG_SUPPRESS = true
		ensureColumns([
			'titleEn',
			'titleRu',
			'gnUrl',
			'alternativeUrl',
			'url',
			'source',
			contentMethodColumn,
			metaTitleColumn,
			metaDescriptionColumn,
			metaKeywordsColumn,
			metaDateColumn,
			metaCanonicalUrlColumn,
			metaImageColumn,
			metaAuthorColumn,
			metaSiteNameColumn,
			metaSectionColumn,
			metaTagsColumn,
			metaLangColumn,
			verifyStatusColumn,
		])

		let list = news.filter(e => String(e[verifyStatusColumn] || '').toLowerCase() !== 'ok')
		progressTracker = createProgressTracker(list.length)

		let stats = { ok: 0, fail: 0 }
		let failures = []
		cacheUrlAliases = buildCacheAliasIndex()
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
			progressTracker?.start(e, i)
			if (isBlank(e.gnUrl)) {
				if (await backfillGnUrl(e, last, { logEvent })) backfilledGn++
			}
			if (isBlank(e.gnUrl) || isBlank(e.titleEn) || isBlank(e.source)) {
				await hydrateFromGoogleNews(e, last, { decodeUrl, logEvent })
			}
			captureOriginalContext(e, e)
			e.gnUrl = normalizeUrl(e.gnUrl)
			if ((isBlank(e.url) || e.url === '') && !isBlank(e.gnUrl) && (!e.text || e.text.length <= minTextLength)) {
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
			e.url = normalizeUrl(e.url)

			let cacheResult = await tryCache(e, e.url, { isFallback: false, origin: 'original', last, contentSource: e.source })
			if (cacheResult?.cacheMetaHit) backfilled++
			let needsTextFields = isBlank(e.summary) || isBlank(e.titleRu) || isBlank(e.topic) || isBlank(e.priority)
			let hasText = e.text?.length > minTextLength
			let contentMethod = e[contentMethodColumn] || e._contentMethod || base?.[contentMethodColumn] || ''
			let methodNote = contentMethod ? `| method=${contentMethod}` : ''
			log(`\n#${e.id} [${i + 1}/${list.length}]`, `${titleFor(e)} ${methodNote}`.trim())

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
					let result = await fetchTextWithRetry(e, e.url, last, { origin: 'original' })
					if (result?.ok) {
						log('got', result.text.length, 'chars')
						saveArticle(e, result.html, result.text, result.url || e.url || '')
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
					let alternatives = getAlternativeArticles(e)
					if (!alternatives.length) {
						await hydrateFromGoogleNews(e, last, { decodeUrl, logEvent })
					}
					let classified = classifyAlternativeCandidates(e)
					alternatives = classified.accepted
					if (classified.accepted.length || classified.rejected.length) {
						for (let alt of classified.accepted) {
							logCandidateDecision(e, alt, 'accepted', '', { phase: 'fallback_candidate' })
						}
						for (let alt of classified.rejected) {
							logCandidateDecision(e, alt, 'rejected', alt.reason || 'filtered', { phase: 'fallback_candidate' })
						}
					}

					if (!alternatives.length) {
						logEvent(e, {
							phase: 'fallback_candidates',
							status: 'empty',
						}, `#${e.id} no fallback candidates`, 'warn')
					}

					let deferredCandidate = null
					const logDeferredCandidates = process.env.LOG_DEFERRED === '1'
					const tryAlternative = async (alt, { allowWait }) => {
						let altUrl = normalizeUrl(alt.url)
						let decodeMethod = altUrl ? 'direct' : 'gn'
						if (!altUrl && alt.gnUrl) {
							if (!allowWait) {
								let waitMs = (last.urlDecode.time + last.urlDecode.delay) - Date.now()
								if (waitMs > 0) {
									if (logDeferredCandidates) {
										logEvent(e, {
											phase: 'fallback_decode',
											status: 'deferred',
											candidateSource: alt.source,
											level: alt.level,
											method: decodeMethod,
											waitMs,
										}, `#${e.id} fallback decode deferred (${alt.source})`, 'info')
									}
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
							logCandidateDecision(e, alt, 'rejected', 'decode_fail', { phase: 'fallback_attempt' })
							return { fetched: false }
						}
						log('Trying alternative source', alt.source, `(level ${alt.level})...`)
						logCandidateDecision(e, alt, 'attempt', '', { phase: 'fallback_attempt' })
						let origin = alt.origin || alt.provider || alt.from || ''
						logEvent(e, {
							phase: 'fallback_decode',
							status: 'ok',
							candidateSource: alt.source,
							level: alt.level,
							method: decodeMethod,
							url: altUrl,
						}, `#${e.id} fallback url decoded (${alt.source})`, 'ok')

						let cacheResult = await tryCache(e, altUrl, { isFallback: true, origin, last, contentSource: alt.source || '' })
						if (cacheResult?.ok) {
							applyFallbackSelection(e, alt, altUrl)
							fetched = true
							logEvent(e, {
								phase: 'fallback_selected',
								status: 'ok',
								candidateSource: alt.source,
								level: alt.level,
							}, `#${e.id} fallback selected ${alt.source}`, 'ok')
							logCandidateDecision(e, alt, 'selected', '', { phase: 'fallback_attempt' })
							return { fetched: true, cached: true }
						}

						let result = await fetchTextWithRetry(e, altUrl, last, { isFallback: true, origin })
						if (result?.ok) {
							applyFallbackSelection(e, alt, altUrl)
							log('got', result.text.length, 'chars')
							saveArticle(e, result.html, result.text, result.url || altUrl || '')
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
							logCandidateDecision(e, alt, 'selected', '', { phase: 'fallback_attempt' })
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
							logCandidateDecision(e, alt, 'rejected', 'verify_mismatch', { phase: 'fallback_attempt' })
						} else {
							let reason = result?.blocked
								? `blocked_${result?.status || 'unknown'}`
								: (result?.rateLimited ? 'rate_limited' : 'no_text')
							logCandidateDecision(e, alt, 'rejected', reason, { phase: 'fallback_attempt' })
						}
						return { fetched: false }
					}

					for (let j = 0; j < alternatives.length; j++) {
						let alt = alternatives[j]
						let res = await tryAlternative(alt, { allowWait: false })
						if (res?.deferred) {
							deferredCandidate = alt
							break
						}
						if (res?.fetched) break
					}
					if (!fetched && deferredCandidate) {
						let res = await tryAlternative(deferredCandidate, { allowWait: true })
						if (res?.fetched) fetched = true
					}
					if (!fetched) {
						let externalResults = []
						if (!externalSearch?.enabled) {
							logEvent(e, {
								phase: 'external_search',
								status: 'skipped',
								reason: 'disabled',
								provider: externalSearch?.provider || '',
							}, `#${e.id} external search skipped (disabled)`, 'warn')
						} else if (!externalSearch.apiKey) {
							logEvent(e, {
								phase: 'external_search',
								status: 'skipped',
								reason: 'missing_api_key',
								provider: externalSearch.provider,
							}, `#${e.id} external search skipped (missing api key)`, 'warn')
						} else {
							let queries = buildFallbackSearchQueries(e)
							if (!queries.length) {
								logEvent(e, {
									phase: 'external_search',
									status: 'skipped',
									reason: 'no_queries',
									provider: externalSearch.provider,
								}, `#${e.id} external search skipped (no queries)`, 'warn')
							} else {
								for (let query of queries) {
									logSearchQuery(e, { phase: 'external_search', provider: externalSearch.provider, query })
									let results = await searchExternal(query)
									logSearchResults(e, { phase: 'external_search', provider: externalSearch.provider, query, results })
									if (results.length) externalResults.push(...results)
								}
							}
						}
						if (externalResults.length && !fetched) {
							let externalClassified = classifyAlternativeCandidates(e, externalResults)
							let externalAlternatives = externalClassified.accepted
							for (let alt of externalClassified.accepted) {
								logCandidateDecision(e, alt, 'accepted', '', { phase: 'external_candidate', provider: externalSearch?.provider || '' })
							}
							for (let alt of externalClassified.rejected) {
								logCandidateDecision(e, alt, 'rejected', alt.reason || 'filtered', { phase: 'external_candidate', provider: externalSearch?.provider || '' })
							}
							if (externalAlternatives.length) {
								let deferredExternal = null
								for (let alt of externalAlternatives) {
									let res = await tryAlternative(alt, { allowWait: false })
									if (res?.deferred) {
										deferredExternal = alt
										break
									}
									if (res?.fetched) {
										fetched = true
										break
									}
								}
								if (!fetched && deferredExternal) {
									let res = await tryAlternative(deferredExternal, { allowWait: true })
									if (res?.fetched) fetched = true
								}
							}
						}
					}
					if (!fetched) {
						logEvent(e, {
							phase: 'fallback_failed',
							status: 'fail',
						}, `#${e.id} fallback exhausted`, 'warn')
					}
				}
			}

			if (needsTextFields && e.text?.length > minTextLength) {
				let aiWaitMs = last.ai.time + last.ai.delay - Date.now()
				if (aiWaitMs > 0) {
					progressTracker?.step(e, 'summarize', 'wait')
					await sleep(aiWaitMs)
				}
				last.ai.time = Date.now()
				log('Summarizing', e.text.length, 'chars...')
				let summarizeStart = Date.now()
				progressTracker?.step(e, 'summarize', 'start')
				let res = await ai({
					url: e.url,
					text: e.text,
					titleEn: e.titleEn,
					titleRu: e.titleRu,
					source: e.source,
					id: e.id,
					meta: e._contentMeta,
				})
				let summarizeMs = Date.now() - summarizeStart
				logEvent(e, {
					phase: 'summarize',
					status: res ? 'ok' : 'empty',
					durationMs: summarizeMs,
					inputChars: e.text?.length || 0,
					outputChars: res?.summary?.length || 0,
				}, `#${e.id} summarize ${summarizeMs}ms`, res ? 'info' : 'warn')
				if (res?.model) {
					progressTracker?.setSummarizeModel?.(e, res.model)
				}
				progressTracker?.setDuration?.(e, 'summarize', summarizeMs)
				progressTracker?.step(e, 'summarize', res ? 'ok' : 'fail', res?.summary ? truncate(res.summary, 160) : undefined)
				if (res) {
					last.ai.delay = res.delay
					e.topic ||= topicsMap[res.topic]
					e.priority ||= res.priority
					e.titleRu ||= res.titleRu
					if (isBlank(e.summary)) e.summary = res.summary
					if (isBlank(e.aiTopic)) e.aiTopic = topicsMap[res.topic]
					if (isBlank(e.aiPriority)) e.aiPriority = res.priority
				}
			} else if (!needsTextFields) {
				progressTracker?.step(e, 'summarize', 'skipped', 'already filled')
			}

			progressTracker?.flushSubsteps?.(e)
			if (!e.summary) {
				logEvent(e, {
					phase: 'summary',
					status: 'missing',
				}, `#${e.id} summary missing`, 'warn')
				progressTracker?.step(e, 'summarize', 'fail', 'summary missing')
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
				progressTracker?.step(e, 'write', 'start')
				let saved = await saveRowByIndex(rowIndex + 1, base)
				progressTracker?.step(e, 'write', saved ? 'ok' : 'fail')
			} else {
				log(`[warn] #${e.id} row index not found; save skipped`)
				progressTracker?.step(e, 'write', 'fail', 'row index not found')
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
		let copyStatusLine = ''
		if (isSummarizeCli) {
			if (!coffeeTodayFolderId) {
				copyStatusLine = 'copy spreadsheet: skipped (coffeeTodayFolderId not set)'
			} else {
				try {
					await copyFile(spreadsheetId, coffeeTodayFolderId, 'news-today')
					copyStatusLine = `copy spreadsheet: ${spreadsheetId} -> ${coffeeTodayFolderId} (ok)`
				} catch (e) {
					let status = e?.status || e?.code
					let reason = e?.errors?.[0]?.reason
					let message = e?.errors?.[0]?.message || e?.message || ''
					if (status === 404 || reason === 'notFound') {
						copyStatusLine = 'copy spreadsheet: skipped (folder not found or no access)'
					} else {
						let suffix = [
							status ? `status=${status}` : '',
							reason ? `reason=${reason}` : '',
							message ? `msg=${message}` : '',
						].filter(Boolean).join(' ')
						copyStatusLine = `copy spreadsheet: failed${suffix ? ` (${suffix})` : ''}`
					}
				}
			}
		}
		let runMs = Date.now() - runStart
		let footer = [
			`sheet read/write: ${spreadsheetId} (sheet=${newsSheet}, mode=${spreadsheetMode})`,
			logging.fetchLogFile ? `fetch log: ${logging.fetchLogFile}` : '',
			`run time: ${formatDuration(runMs) || `${Math.round(runMs)}ms`}`,
			copyStatusLine,
		]
		progressTracker?.setFooter?.(footer)
		progressTracker?.done?.()
		log('\n', stats)
		return stats
	} catch (error) {
		if (error?.code === 'BROWSER_CLOSED') {
			log('[fatal] browser window closed; stopping summarize')
		}
		throw error
	} finally {
		globalThis.__LOG_SUPPRESS = false
		if (!isSummarizeCli) globalThis.__LOG_SUPPRESS_ALL = wasSuppressAll
		await resumeAutoSave({ flush: false })
	}
}

if (process.argv[1].endsWith('summarize')) {
	;(async () => {
		await summarize()
	})()
}
