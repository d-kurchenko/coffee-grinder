import OpenAI from 'openai'

import { log } from './log.js'
import { sleep } from './sleep.js'
import { getPrompt, PROMPTS } from './ai-prompts.js'
import { logging } from '../config/logging.js'
import { logFetch } from './fetch-log.js'

let openai = new OpenAI()
let instructions = getPrompt(PROMPTS.SUMMARIZE)

const summarySchema = {
	name: 'news_summary',
	schema: {
		type: 'object',
		additionalProperties: false,
		properties: {
			topic: { type: 'string' },
			priority: { type: ['string', 'number'] },
			titleRu: { type: 'string' },
			summary: { type: 'string' },
		},
		required: ['topic', 'priority', 'summary', 'titleRu'],
	},
	strict: true,
}

export async function ai({ url, text, titleEn, titleRu, source, id, meta }) {
	await init
	for (let i = 0; i < 3; i++) {
		try {
			let systemContent = [
				instructions,
				'Return ONLY JSON with keys: topic, priority, titleRu, summary.',
				'Use topic values that exist in the provided taxonomy when possible.',
			].join('\n')
			let metaLines = []
			let truncate = (value, max = 300) => {
				if (!value) return ''
				let textValue = String(value)
				return textValue.length > max ? textValue.slice(0, max - 3) + '...' : textValue
			}
			if (meta?.title) metaLines.push(`Meta title: ${truncate(meta.title)}`)
			if (meta?.description) metaLines.push(`Meta description: ${truncate(meta.description, 400)}`)
			if (meta?.keywords) metaLines.push(`Meta keywords: ${truncate(meta.keywords)}`)
			if (meta?.date || meta?.publishedTime) {
				metaLines.push(`Meta date: ${truncate(meta.publishedTime || meta.date)}`)
			}
			if (meta?.author) metaLines.push(`Meta author: ${truncate(meta.author)}`)
			if (meta?.siteName) metaLines.push(`Meta site: ${truncate(meta.siteName)}`)
			if (meta?.section) metaLines.push(`Meta section: ${truncate(meta.section)}`)
			if (meta?.tags) metaLines.push(`Meta tags: ${truncate(meta.tags, 400)}`)
			let userContent = [
				`Title: ${titleEn || titleRu || ''}`,
				`Source: ${source || ''}`,
				`URL: ${url || ''}`,
				metaLines.length ? 'Meta:' : '',
				...metaLines,
				'Text:',
				text || '',
			].join('\n')
			if (logging.includeAiPrompt) {
				let limit = Number.isFinite(logging.aiPromptMaxChars) ? logging.aiPromptMaxChars : 0
				let systemLogged = limit > 0 && systemContent.length > limit
					? systemContent.slice(0, limit)
					: systemContent
				let userLogged = limit > 0 && userContent.length > limit
					? userContent.slice(0, limit)
					: userContent
				logFetch({
					phase: 'ai_prompt',
					eventId: id,
					model: 'gpt-4o',
					temperature: 0.2,
					url,
					source,
					system: systemLogged,
					prompt: userLogged,
				}, 'ai prompt', 'info')
			}
			let model = 'gpt-4o'
			let completion = await openai.chat.completions.create({
				model,
				temperature: 0.2,
				messages: [
					{
						role: 'system',
						content: systemContent,
					},
					{
						role: 'user',
						content: userContent,
					},
				],
				response_format: { type: 'json_schema', json_schema: summarySchema },
			})
			let content = completion?.choices?.[0]?.message?.content || ''
			let res = JSON.parse(content)
			res.model = model
			log('got', res.summary.length, 'chars,', completion.usage?.total_tokens, 'tokens used')
			logFetch({
				phase: 'ai_result',
				eventId: id,
				url,
				source,
				titleEn: titleEn || '',
				titleRu: res.titleRu || titleRu || '',
				summary: res.summary || '',
				topic: res.topic || '',
				priority: res.priority || '',
			}, 'ai result', 'info')
			res.delay = (completion.usage?.total_tokens || 0) / 30e3 * 60e3
			return res
		} catch(e) {
			log('AI fail\n', e)
			await sleep(30e3)
		}
	}
}
