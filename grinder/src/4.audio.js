import OpenAI from 'openai'
import { readFileSync } from 'fs'
import levenshtein from 'fast-levenshtein'

import { log } from './log.js'
import { news } from './store.js'
import { speak } from './eleven.js'
import { uploadFolder } from './google-drive.js'
import { coffeeTodayFolderId, audioFolderName } from '../config/google-drive.js'
import { withRetry } from './retry.js'
import { getPrompt, PROMPTS } from './ai-prompts.js'

const openai = new OpenAI()

const MAX_VALIDATION_RETRIES = 2
const VALIDATION_RETRY_DELAY = 2000
const HIGH_SIMILARITY_THRESHOLD = 0.96
const MIN_SIMILARITY_THRESHOLD = 0.8

/**
 * Normalizes text for comparison (removes punctuation, extra spaces, lowercase)
 */
function normalizeText(text) {
	return text.toLowerCase()
		.replace(/[.,\/#!$%\^&\*;:{}=\-_'"`~()]/g, "")
		.replace(/\s{2,}/g, " ")
		.trim()
}

async function transcribeAndValidate(filePath, originalText) {
	const transcriptionPrompt = await getPrompt(PROMPTS.AUDIO_TRANSCRIPTION)
	const validationPrompt = await getPrompt(PROMPTS.TRANSCRIPTION_VALIDATION)

	log('Transcribing audio...')
	const audioBuffer = readFileSync(filePath)
	const base64Audio = audioBuffer.toString('base64')

	// 1. Get transcription with retry for API errors
	const transcriptionResponse = await withRetry(async () => {
		return await openai.chat.completions.create({
			model: "gpt-4o-mini-audio-preview",
			temperature: 0,
			messages: [
				{ role: "system", content: transcriptionPrompt },
				{
					role: "user",
					content: [
						{
							type: "input_audio",
							input_audio: {
								data: base64Audio,
								format: "mp3"
							}
						}
					]
				}
			]
		})
	}, { retries: 2, delay: 3000, label: 'Transcription API' })

	const transcription = transcriptionResponse.choices[0].message.content
	const transcriptionPreview = transcription.length > 100
		? transcription.substring(0, 100) + '...'
		: transcription
	log('Transcription received:', transcriptionPreview)

	const normalizedOriginal = normalizeText(originalText)
	const normalizedTranscription = normalizeText(transcription)

	// 2. Levenshtein check
	if (!normalizedOriginal || !normalizedTranscription) return false
	const distance = levenshtein.get(normalizedOriginal, normalizedTranscription)
	const maxLen = Math.max(normalizedOriginal.length, normalizedTranscription.length)
	const similarity = 1 - (distance / maxLen)
	log(`Similarity: ${(similarity * 100).toFixed(2)}%`)

	if (similarity >= HIGH_SIMILARITY_THRESHOLD) {
		log('Levenshtein check passed (High similarity).')
		return true
	}

	if (similarity < MIN_SIMILARITY_THRESHOLD) {
		log('Levenshtein check failed (Similarity too low).')
		return false
	}

	// 3. AI detailed validation if similarity is in the uncertain range (MIN to HIGH)
	log('Similarity in uncertain range, performing detailed AI validation...')
	const aiValidationResponse = await withRetry(async () => {
		return await openai.chat.completions.create({
			model: "gpt-4o-mini",
			temperature: 0.25,
			messages: [
				{ role: "system", content: validationPrompt },
				{
					role: "user",
					content: `Text A (Original):\n${normalizedOriginal}\n\nText B (Transcription):\n${normalizedTranscription}`
				}
			],
			response_format: { type: "json_object" }
		})
	}, { retries: 2, delay: 3000, label: 'Validation API' })

	try {
		const result = JSON.parse(aiValidationResponse.choices[0].message.content)
		log('AI Validation result:', result)
		return result.isValid === true
	} catch (e) {
		log('Failed to parse AI validation response, falling back to Levenshtein result.')
		return false
	}
}

export async function audio() {
	let list = news.filter(e => e.sqk && e.summary)
	for (let i = 0; i < list.length; i++) {
		let event = list[i]
		log(`\n[${i + 1}/${list.length}]`, `${event.sqk}. ${event.titleEn || event.titleRu}`)

		if (event.summary) {
			const filePath = `../${audioFolderName}/${event.sqk}.mp3`

			const generateAndValidate = async () => {
				try {
					await withRetry(
						() => speak(filePath, event.summary),
						{ retries: 3, delay: 2000, label: 'ElevenLabs API' }
					)
				} catch (e) {
					log(`Critical ElevenLabs failure, skipping: ${e.message}`)
					return
				}

				try {
					const isValid = await transcribeAndValidate(filePath, event.summary)
					if (isValid === false) {
						throw new Error('VALIDATION_FAILED')
					}
				} catch (e) {
					if (e.message === 'VALIDATION_FAILED') {
						log('Audio validation failed.')
						throw e
					}
					log(`Validation process failed, skipping: ${e.message}`)
				}
			}

			try {
				await withRetry(
					generateAndValidate,
					{ retries: MAX_VALIDATION_RETRIES, delay: VALIDATION_RETRY_DELAY, label: 'Audio Validation Flow' }
				)
				log('Audio successfully generated and validated.')
			} catch (e) {
				if (e.message === 'VALIDATION_FAILED') {
					log(`Warning: Audio validation failed after ${MAX_VALIDATION_RETRIES} attempts for "${event.sqk}".`)
				} else {
					log(`Unexpected error in flow for "${event.sqk}":`, e.message)
				}
				log('Proceeding with last version.')
			}
		}
	}

	log('\nUploading audio to Drive...')
	await uploadFolder('../audio', coffeeTodayFolderId, audioFolderName, ['.mp3'])
	log('Audio uploaded.')
}

if (process.argv[1].endsWith('audio')) audio()
