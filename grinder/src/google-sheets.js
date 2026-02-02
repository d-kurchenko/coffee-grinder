import Sheets from '@googleapis/sheets'

import { log } from './log.js'
import { auth } from './google-auth.js'
import { sheetsConfig } from '../config/sheets.js'

let sheets
async function initialize() {
	sheets = await Sheets.sheets({ version: 'v4', auth }).spreadsheets
}
let init = initialize()

export async function load(spreadsheetId, range) {
	await init
	const res = await sheets.values.get({ spreadsheetId, range })
	return res.data.values
}

export async function save(spreadsheetId, range, data) {
	return await sheets.values.update({
		spreadsheetId,
		range,
		// valueInputOption: 'RAW',
		valueInputOption: 'USER_ENTERED',
		requestBody: { values: data },
	})
}

function columnToLetter(index) {
	let n = index + 1
	let letters = ''
	while (n > 0) {
		let rem = (n - 1) % 26
		letters = String.fromCharCode(65 + rem) + letters
		n = Math.floor((n - 1) / 26)
	}
	return letters
}

function buildRow(headers, row, rowNumber) {
	const maxCellChars = sheetsConfig.maxCellChars
	const dropOversize = sheetsConfig.dropOversize
	const oversize = []
	const values = []
	let rowId = row?.id || row?.sqk || row?.url || ''
	for (let h of headers) {
		let value = row?.[h]
		if (value === null || value === undefined) {
			values.push('')
			continue
		}
		if (typeof value === 'string' && value.length > maxCellChars) {
			oversize.push({
				row: rowNumber,
				column: h,
				length: value.length,
				id: rowId,
			})
			values.push(dropOversize ? '' : value)
			continue
		}
		values.push(value)
	}
	return { values, oversize, dropOversize }
}

export async function loadTable(spreadsheetId, range) {
	const rows = await load(spreadsheetId, range)
	// log('Data from sheet:', rows)
	const headers = rows[0]
	const data = rows.slice(1).map(row => {
		let obj = {}
		row.forEach((cell, i) => {
			obj[headers[i]] = cell
		})
		return obj
	})
	data.headers = headers
	return data
}

export async function saveRow(spreadsheetId, sheet, headers, rowNumber, row) {
	await init
	const { values, oversize, dropOversize } = buildRow(headers, row, rowNumber)
	if (oversize.length) {
		log(`Oversized cells detected (${oversize.length}). Max is ${sheetsConfig.maxCellChars} chars.`)
		for (let item of oversize.slice(0, sheetsConfig.oversizeLogLimit)) {
			log(`Oversized cell row ${item.row} col "${item.column}" len ${item.length}${item.id ? ` id ${item.id}` : ''}`)
		}
		if (oversize.length > sheetsConfig.oversizeLogLimit) {
			log(`Oversized cell log truncated (${oversize.length - sheetsConfig.oversizeLogLimit} more)`)
		}
		if (!dropOversize) {
			throw new Error('Oversized cell(s) exceed Google Sheets limit. Set SHEETS_DROP_OVERSIZE=1 or sheetsConfig.dropOversize to drop.')
		}
	}
	const lastColumn = columnToLetter(Math.max(0, headers.length - 1))
	const range = `${sheet}!A${rowNumber}:${lastColumn}${rowNumber}`
	return await sheets.values.update({
		spreadsheetId,
		range,
		valueInputOption: 'USER_ENTERED',
		requestBody: { values: [values] },
	})
}

export async function saveTable(spreadsheetId, range, data) {
	let { headers } = data
	await init
	const maxCellChars = sheetsConfig.maxCellChars
	const dropOversize = sheetsConfig.dropOversize
	const oversize = []
	const updatedData = [headers]

	for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
		let row = []
		let o = data[rowIndex]
		let rowId = o?.id || o?.sqk || o?.url || ''
		for (let h of headers) {
			let value = o?.[h]
			if (value === null || value === undefined) {
				row.push('')
				continue
			}
			if (typeof value === 'string' && value.length > maxCellChars) {
				oversize.push({
					row: rowIndex + 2,
					column: h,
					length: value.length,
					id: rowId,
				})
				row.push(dropOversize ? '' : value)
				continue
			}
			row.push(value)
		}
		updatedData.push(row)
	}

	if (oversize.length) {
		log(`Oversized cells detected (${oversize.length}). Max is ${maxCellChars} chars.`)
		for (let item of oversize.slice(0, sheetsConfig.oversizeLogLimit)) {
			log(`Oversized cell row ${item.row} col "${item.column}" len ${item.length}${item.id ? ` id ${item.id}` : ''}`)
		}
		if (oversize.length > sheetsConfig.oversizeLogLimit) {
			log(`Oversized cell log truncated (${oversize.length - sheetsConfig.oversizeLogLimit} more)`)
		}
		if (!dropOversize) {
			throw new Error('Oversized cell(s) exceed Google Sheets limit. Set SHEETS_DROP_OVERSIZE=1 or sheetsConfig.dropOversize to drop.')
		}
	}
	// log({ updatedData })
	return await save(spreadsheetId, range, updatedData)
}
