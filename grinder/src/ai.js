import OpenAI from 'openai'

import { log } from './log.js'
import { sleep } from './sleep.js'
import { getPrompt, PROMPTS } from './ai-prompts.js'

let openai = new OpenAI()
let assistant
async function initialize() {
	let instructions = await getPrompt(PROMPTS.SUMMARIZE)
	assistant = await openai.beta.assistants.create({
		name: "Summarizer",
		instructions,
		model: "gpt-4o",
	})
}
let init = initialize()

export async function ai({ url, text }) {
	await init
	for (let i = 0; i < 3; i++) {
		let thread = await openai.beta.threads.create()
		let content = `URL: ${url}\nText:\n${text}`
		const message = await openai.beta.threads.messages.create(thread.id, {
			role: "user",
			content,
		})
		try {
			let run = await openai.beta.threads.runs.createAndPoll(thread.id, {
				assistant_id: assistant.id,
			})
			if (run?.status === 'completed') {
				const messages = await openai.beta.threads.messages.list(run.thread_id)
				// log(run)
				// log(messages.data[0].content)
				let json = messages.data[0].content[0].text.value.replace('```json', '').replace('```', '')
				try {
					let res = JSON.parse(json)
					log('got', res.summary.length, 'chars,', run.usage.total_tokens, 'tokens used')
					res.delay = run.usage.total_tokens / 30e3 * 60e3
					return res
				} catch (e) {
					log('AI fail\n', json, '\n', e)
					return null
				}
			} else {
				log('AI fail\n', run?.last_error || run)
				await sleep(30e3)
			}
		} catch(e) {
			log('AI fail\n', e)
			await sleep(30e3)
		}
	}
}