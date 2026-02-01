import { loadTable } from './google-sheets.js'
import { spreadsheetId } from './store.js'
import { aiSheet } from '../config/google-drive.js'
import { log } from './log.js'

export const PROMPTS = {
	SUMMARIZE: 'summarize',
	AUDIO_TRANSCRIPTION: 'audio-transcription',
	TRANSCRIPTION_VALIDATION: 'transcription-validation'
}

let prompts = null

export async function getPrompt(name) {
	if (!prompts) {
		log('Loading AI prompts...')
		const data = await loadTable(spreadsheetId, aiSheet)
		prompts = {}
		for (const row of data) {
			if (row.name && row.prompt) {
				prompts[row.name] = row.prompt
			}
		}
	}
	const p = prompts[name]
	if (!p) {
		throw new Error(`Critical Error: Prompt "${name}" not found in sheet "${aiSheet}". Available: ${Object.keys(prompts).join(', ')}`)
	}
	return p
}
