export let rootFolderId = '1nJnHBcY252xqV0JDIidsxxhJkY6-w2WO'
export let mainSpreadsheetId =
	process.env.GOOGLE_SHEET_ID_MAIN ||
	'1q7UmHBzY92JVan2dkeMqxHSvWSMYjUJ1gD0CxisPOdI'
export let autoSpreadsheetId =
	process.env.GOOGLE_SHEET_ID_AUTO ||
	'1q7UmHBzY92JVan2dkeMqxHSvWSMYjUJ1gD0CxisPOdI' // table with manually added news
export const coffeeTodayFolderId =
	process.env.COFFEE_TODAY_FOLDER_ID ||
	process.env.GOOGLE_DRIVE_COFFEE_TODAY_FOLDER_ID ||
	'1P1m2QdN_Kefr_k2Kz4foYa0EmmQ0Acw_' // folder to put all files
export let newsSheet = 'news'
export let aiSheet = 'ai-instructions'
export let templatePresentationId = '1dmPEq5CKOguEtFzeSPt26_6xghrGe7LHSAyaxCLx3uY' // presentation template
export let templateSlideId = 'p'
export let templateTableId = 'g32d1f8c10a8_0_0'
export let presentationName = 'coffee-maker'
export let autoPresentationName = 'coffee-maker-auto'
export let audioFolderName = 'audio'
export let imageFolderName = 'img'
export let archiveFolderId = '1pNa15MULvOIaGqeQAYfxN9e7FX5cLlgR' // move coffeeTodayFolder here before running grinder
export let autoArchiveFolderId = '17OlCbhRNhkLYSL6aKYMAr_d2oazw_RaX'
