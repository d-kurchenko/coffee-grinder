import { chromium } from 'playwright'
import os from 'os'
import { dirname } from 'path'
import { fileURLToPath } from 'url'

import { log } from './log.js'
import { sleep } from './sleep.js'
import { isDomainInCooldown } from './domain-cooldown.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

async function initialize() {
	const platform = os.platform()
	let chromeExecutablePath
	let chromeProfilePath

	if (platform === 'darwin') { // macOS
		chromeExecutablePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
		chromeProfilePath = `${os.homedir()}/Library/Application Support/Google/Chrome/Playwright Profile`
	} else if (platform === 'win32') { // Windows
		chromeExecutablePath = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
		chromeProfilePath = `${os.homedir()}\\AppData\\Local\\Google\\Chrome\\Playwright Profile`
	} else { // Linux and others
		chromeExecutablePath = '/usr/bin/google-chrome' // or '/usr/bin/chromium-browser'
		chromeProfilePath = `${os.homedir()}/.config/google-chrome/Playwright Profile`
	}

	// log(`Chrome profile path: ${chromeProfilePath}`)
	let extension = `${__dirname}/../extensions/captcha-solver/0.2.1_0`
	let context = await chromium.launchPersistentContext(chromeProfilePath, {
		headless: false,
		executablePath: chromeExecutablePath,
		viewport: { width: 1024, height: 600 },
		screen: { width: 1024, height: 600 },
		args: [
			'--start-maximized',
			`--disable-extensions-except=${extension}`,
			`--load-extension=${extension}`,
		],
	})
	let page = await context.newPage()
	return { context, page }
}
let init = initialize()

function isBrowserClosedError(error) {
	let message = String(error?.message || error || '').toLowerCase()
	return message.includes('target page') && message.includes('has been closed')
		|| message.includes('context or browser has been closed')
		|| message.includes('browser has been closed')
		|| message.includes('target closed')
		|| error?.name === 'TargetClosedError'
}

export async function finalyze() {
	let { context } = await init
	context?.close()
}

function toBrowseError(error) {
	if (isBrowserClosedError(error)) {
		let err = new Error('Playwright browser window is closed')
		err.code = 'BROWSER_CLOSED'
		return err
	}
	let message = String(error?.message || error || '')
	let err = new Error(`Browse failed: ${message}`)
	err.code = 'BROWSE_ERROR'
	return err
}

export async function browseArticle(url) {
	let { page } = await init
	try {
		if (page?.isClosed?.()) {
			let err = new Error('Playwright browser window is closed')
			err.code = 'BROWSER_CLOSED'
			throw err
		}
		log('Browsing archive...')
		await page.goto(`https://archive.ph/${url.split('?')[0]}`, {
			waitUntil: 'load',
		})

		let captcha = await page.$('iframe[src*="recaptcha"]')
		if (captcha) {
			log('waiting for captcha to be solved...')
			await page.waitForSelector('#CONTENT', { timeout: 180e3 })
			log('captcha solved')
		} else {
			log('no captcha detected')
		}

		const versions = await page.$$('.TEXT-BLOCK > a')
		if (versions.length > 0) {
			log('going to the newest version...')
			await versions[0].click()
			await page.waitForLoadState('load')
		}

		let html =  await page.evaluate(() => {
			return [...document.querySelectorAll('.body')].map(x => x.innerHTML).join('')
		})

		if (!html) {
			log('browsing source...')
			let cooldown = isDomainInCooldown(url)
			if (cooldown) {
				log('domain cooldown active', cooldown.host, Math.ceil(cooldown.remainingMs / 1000), 's')
				return ''
			}
			try {
				await page.goto(url, {
					waitUntil: 'load',
					timeout: 10e3,
				})
			} catch (e) {
				if (isBrowserClosedError(e)) {
					let err = new Error('Playwright browser window is closed')
					err.code = 'BROWSER_CLOSED'
					throw err
				}
				log(e)
			}
			try {
				await page.waitForLoadState('networkidle', {
					timeout: 10e3,
				})
			} catch (e) {
				if (isBrowserClosedError(e)) {
					let err = new Error('Playwright browser window is closed')
					err.code = 'BROWSER_CLOSED'
					throw err
				}
				log(e)
			}
			html = await page.evaluate(() => {
				return document.body.innerHTML
			})
		}
		return html
	}
	catch (e) {
		throw toBrowseError(e)
	}
}
