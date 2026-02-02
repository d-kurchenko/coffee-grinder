const dropOversizeEnv = String(process.env.SHEETS_DROP_OVERSIZE || '').toLowerCase()
const dropOversize = dropOversizeEnv === '1' || dropOversizeEnv === 'true' || dropOversizeEnv === 'yes'

export const sheetsConfig = {
	maxCellChars: 50000,
	dropOversize,
	oversizeLogLimit: 20,
}
