import { ElevenLabsClient } from "elevenlabs"
import { createWriteStream } from 'fs'

import { log } from './log.js'

const client = new ElevenLabsClient()
const VOICE_ID = process.env.ELEVEN_VOICE_ID || 'caCdUepOP0tqRkMSyQWB'

export async function speak(filePath, text) {
	return new Promise(async (resolve, reject) => {
		let error = e => {
			log('ElevenLabs error:', e?.message || e)
			log('Error details:', JSON.stringify(e, null, 2))
			reject(e)
		}
		try {
			let audio = await client.textToSpeech.convert(VOICE_ID, {
				text,
				model_id: "eleven_multilingual_v2",
			})
			let fileStream = createWriteStream(filePath)
			audio.pipe(fileStream);
			fileStream.on('finish', () => resolve(true))
			fileStream.on('error', error)
		} catch(e) {
			error(e)
		}
	})
}
