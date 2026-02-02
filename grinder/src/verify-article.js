import {
	verifyMaxChars,
	verifySummaryMaxChars,
	verifyContextMaxChars,
	verifyModel,
	verifyTemperature,
	verifyUseSearch,
	verifyFallbackMaxChars,
	verifyFallbackContextMaxChars,
} from '../config/verification.js'
import { log } from './log.js'

const XAI_API_URL = process.env.XAI_API_URL || 'https://api.x.ai/v1/responses'
const XAI_API_KEY = process.env.XAI_API_KEY || ''

function cleanJsonText(text) {
	if (!text) return ''
	let trimmed = text.trim()
	if (trimmed.startsWith('```')) {
		trimmed = trimmed.replace(/^```(?:json)?/i, '').replace(/```$/, '').trim()
	}
	if (trimmed.startsWith('{') && trimmed.endsWith('}')) return trimmed
	let match = trimmed.match(/\{[\s\S]*\}/)
	return match ? match[0] : trimmed
}

function clampSummary(text) {
	if (!text) return ''
	return text.length > verifySummaryMaxChars ? text.slice(0, verifySummaryMaxChars) : text
}

function clampText(text, limit) {
	if (!text) return ''
	if (!Number.isFinite(limit) || limit <= 0) return text
	return text.length > limit ? text.slice(0, limit) : text
}

function isLengthError(error) {
	let message = String(error?.message || error || '').toLowerCase()
	return message.includes('context')
		|| message.includes('token')
		|| message.includes('too long')
		|| message.includes('maximum')
		|| message.includes('input size')
		|| message.includes('max_tokens')
}

function buildPayload(original, candidate, { maxChars, contextMaxChars }) {
	return {
		original: {
			title: original?.title || '',
			description: original?.description || '',
			keywords: original?.keywords || '',
			date: original?.date || '',
			source: original?.source || '',
			url: original?.url || '',
			gnUrl: original?.gnUrl || '',
			textSnippet: clampText(original?.textSnippet || '', contextMaxChars),
		},
		candidate: {
			url: candidate?.url || '',
			text: clampText(candidate?.text || '', maxChars),
		},
	}
}

function buildPrompt(payload) {
	let system = [
		'You verify whether the candidate article is about the same news event as the original article.',
		'Be strict: only mark match=true if it is clearly the same event.',
		'The candidate may contain MORE information, but must NOT contradict the original.',
		'If the candidate omits key facts from the original or is about a related but different event, set match=false.',
		'Use web_search to confirm details when needed.',
		'Dates and sources may differ slightly, but the event must be the same.',
		'Return ONLY JSON with keys:',
		'- match (boolean)',
		'- confidence (number 0-1)',
		'- reason (string, <=200 chars)',
		'- page_summary (string, <=200 chars)',
	].join(' ')
	let user = [
		'Original context:',
		JSON.stringify(payload.original, null, 2),
		'Candidate:',
		JSON.stringify(payload.candidate, null, 2),
	].join('\n')
	return { system, user }
}

function extractResponseText(response) {
	if (!response) return ''
	if (typeof response.output_text === 'string') return response.output_text
	if (Array.isArray(response.output)) {
		for (let item of response.output) {
			if (item?.type !== 'message') continue
			if (!Array.isArray(item.content)) continue
			let text = item.content
				.filter(part => part && (part.type === 'output_text' || typeof part.text === 'string'))
				.map(part => part.text || '')
				.join('')
			if (text) return text
		}
	}
	let fallback = response?.choices?.[0]?.message?.content
	return typeof fallback === 'string' ? fallback : ''
}

async function callXai({ system, prompt, temperature, useSearch }) {
	if (!XAI_API_KEY) throw new Error('XAI_API_KEY is not set')
	let body = {
		model: verifyModel,
		temperature,
		input: [
			{ role: 'system', content: system },
			{ role: 'user', content: prompt },
		],
	}
	if (useSearch) {
		body.tools = [{ type: 'web_search' }]
	}
	let response = await fetch(XAI_API_URL, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'Authorization': `Bearer ${XAI_API_KEY}`,
		},
		body: JSON.stringify(body),
	})
	let data = await response.json().catch(() => ({}))
	if (!response.ok) {
		let message = data?.error?.message || data?.message || response.statusText
		throw new Error(`xAI API error: ${message}`)
	}
	return data
}

export async function verifyArticle({
	original,
	candidate,
	minConfidence,
	failOpen,
	debug,
	debugMaxChars,
}) {
	let payload = buildPayload(original, candidate, {
		maxChars: verifyMaxChars,
		contextMaxChars: verifyContextMaxChars,
	})
	try {
		let { system, user } = buildPrompt(payload)
		let debugSystem = ''
		let debugUser = ''
		if (debug) {
			debugSystem = clampText(system, debugMaxChars)
			debugUser = clampText(user, debugMaxChars)
		}

		let completion
		let fallbackUsed = false
		try {
			completion = await callXai({
				system,
				prompt: user,
				temperature: verifyTemperature,
				useSearch: verifyUseSearch,
			})
		} catch (error) {
			if (!isLengthError(error)) throw error
			fallbackUsed = true
			let fallbackPayload = buildPayload(original, candidate, {
				maxChars: verifyFallbackMaxChars,
				contextMaxChars: verifyFallbackContextMaxChars,
			})
			let fallbackPrompt = buildPrompt(fallbackPayload)
			if (debug) {
				debugSystem = clampText(fallbackPrompt.system, debugMaxChars)
				debugUser = clampText(fallbackPrompt.user, debugMaxChars)
			}
			completion = await callXai({
				system: fallbackPrompt.system,
				prompt: fallbackPrompt.user,
				temperature: verifyTemperature,
				useSearch: verifyUseSearch,
			})
		}
		let content = extractResponseText(completion)
		let jsonText = cleanJsonText(content)
		let parsed = JSON.parse(jsonText)
		let match = Boolean(parsed.match)
		let confidence = Number(parsed.confidence ?? 0)
		let reason = clampSummary(String(parsed.reason ?? ''))
		let pageSummary = clampSummary(String(parsed.page_summary ?? parsed.pageSummary ?? ''))
		let ok = match && confidence >= minConfidence
		return {
			ok,
			match,
			confidence,
			reason,
			pageSummary,
			verified: true,
			status: ok ? 'ok' : 'mismatch',
			tokens: completion?.usage?.total_tokens ?? completion?.usage?.totalTokens,
			debug: debug
				? {
					model: verifyModel,
					temperature: verifyTemperature,
					useSearch: verifyUseSearch,
					fallbackUsed,
					fallbackMaxChars: verifyFallbackMaxChars,
					fallbackContextMaxChars: verifyFallbackContextMaxChars,
					system: debugSystem,
					prompt: debugUser,
				}
				: undefined,
			fallbackUsed,
		}
	} catch (error) {
		log('verify failed', error)
		if (failOpen) {
			return {
				ok: true,
				match: false,
				confidence: 0,
				reason: 'verification unavailable',
				pageSummary: '',
				verified: false,
				status: 'unverified',
				error,
			}
		}
		return {
			ok: false,
			match: false,
			confidence: 0,
			reason: 'verification failed',
			pageSummary: '',
			verified: false,
			status: 'error',
			error,
		}
	}
}
