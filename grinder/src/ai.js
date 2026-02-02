import OpenAI from 'openai'

import { spreadsheetId } from './store.js'
import { log } from './log.js'
import { sleep } from './sleep.js'
import { load } from './google-sheets.js'
import { aiSheet } from '../config/google-drive.js'
import { logging } from '../config/logging.js'
import { logFetch } from './fetch-log.js'

let openai = new OpenAI()
let instructions = ''
async function initialize() {
	let rows = await load(spreadsheetId, aiSheet)
	if (!rows || !rows.length) {
		log('AI instructions sheet is missing or empty:', aiSheet)
		rows = [['You are a news summarizer. Return concise results.']]
	}
	instructions = rows.map(x => x.join('\t')).join('\n')
}
let init = initialize()

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

export async function ai({ url, text, titleEn, titleRu, source, id }) {
	await init
	for (let i = 0; i < 3; i++) {
		try {
			let systemContent = [
				instructions,
				'Return ONLY JSON with keys: topic, priority, titleRu, summary.',
				'Use topic values that exist in the provided taxonomy when possible.',
			].join('\n')
			let userContent = [
				`Title: ${titleEn || titleRu || ''}`,
				`Source: ${source || ''}`,
				`URL: ${url || ''}`,
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
			let completion = await openai.chat.completions.create({
				model: 'gpt-4o',
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
