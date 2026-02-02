import { proxy, subscribe } from 'valtio/vanilla'

import { log } from './log.js'
import { loadTable, saveTable, saveRow } from './google-sheets.js'
import { mainSpreadsheetId, autoSpreadsheetId, newsSheet } from '../config/google-drive.js'

export let spreadsheetMode = process.argv[2]?.endsWith('auto') ? 'auto' : 'main'
export let spreadsheetId = spreadsheetMode === 'auto' ? autoSpreadsheetId : mainSpreadsheetId

export let news = []
// try {
// 	news = JSON.parse(fs.readFileSync('news.json', 'utf8'))
// } catch(e) {}
// news = proxy(news)
// subscribe(news, () => fs.writeFileSync('news.json', JSON.stringify(news, null, 2)))
news = proxy(await loadTable(spreadsheetId, newsSheet))
subscribe(news, () => queueSave('auto'))

const saveDebounceMs = Math.max(200, Number.parseInt(process.env.SHEETS_SAVE_DEBOUNCE_MS || '', 10) || 2000)
let saveTimer = null
let saveInProgress = false
let pendingSave = false
let autoSavePaused = false
let savePromise = null

function snapshotTable(data) {
	let rows = data.map(row => ({ ...row }))
	rows.headers = Array.isArray(data.headers) ? data.headers.slice() : []
	return rows
}

function formatSaveError(error) {
	if (!error) return 'unknown error'
	let message = error.message || String(error)
	let status = error?.response?.status || error?.status || error?.code
	let reason = error?.errors?.[0]?.reason
	let detail = error?.errors?.[0]?.message
	let parts = [message]
	if (status) parts.push(`status ${status}`)
	if (reason) parts.push(reason)
	if (detail && detail !== message) parts.push(detail)
	return parts.join(' | ')
}

function formatSaveErrorInline(error) {
	if (!error) return ''
	let status = error?.response?.status || error?.status || error?.code
	let reason = error?.errors?.[0]?.reason
	let message = error?.errors?.[0]?.message || error?.message || ''
	let parts = []
	if (status) parts.push(`status=${status}`)
	if (reason) parts.push(`reason=${reason}`)
	if (message) parts.push(`msg=${message}`)
	return parts.join(' ')
}

function queueSave(reason = '') {
	pendingSave = true
	if (autoSavePaused) return
	if (saveTimer) return
	saveTimer = setTimeout(() => {
		saveTimer = null
		void flushSave({ reason })
	}, saveDebounceMs)
}

export function pauseAutoSave() {
	autoSavePaused = true
}

export async function resumeAutoSave({ flush = true } = {}) {
	autoSavePaused = false
	if (flush) await flushSave({ bypassPause: true, reason: 'resume' })
}

export async function flushSave({ force = false, bypassPause = false, reason = '' } = {}) {
	if (saveInProgress) {
		pendingSave = true
		return savePromise
	}
	if (!pendingSave && !force) return
	if (autoSavePaused && !bypassPause) return
	pendingSave = false
	saveInProgress = true
	let snapshot = snapshotTable(news)
	savePromise = (async () => {
		try {
			await saveTable(spreadsheetId, newsSheet, snapshot)
		} catch (e) {
			log('Failed to save', formatSaveError(e))
		}
	})()
	try {
		return await savePromise
	} finally {
		saveInProgress = false
		savePromise = null
		if (pendingSave && !autoSavePaused) queueSave(reason)
	}
}

export async function save() {
	try {
		// log('Saving...')
		await saveTable(spreadsheetId, newsSheet, snapshotTable(news))
		// log('saved')
	} catch(e) {
		log('Failed to save', formatSaveError(e))
	}
}

export async function saveRowByIndex(rowNumber, row) {
	try {
		await saveRow(spreadsheetId, newsSheet, news.headers || [], rowNumber, row)
	} catch (e) {
		let detail = formatSaveErrorInline(e)
		log(`[warn] sheet write failed (sheet=${spreadsheetId} tab=${newsSheet} row=${rowNumber}${detail ? ` ${detail}` : ''})`)
	}
}
