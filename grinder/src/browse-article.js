import { chromium } from 'playwright'
import os from 'os'
import { dirname } from 'path'
import { fileURLToPath } from 'url'

import { log } from './log.js'
import { sleep } from './sleep.js'
import { isDomainInCooldown, setDomainCooldown } from './domain-cooldown.js'

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
const captchaCooldownMs = 10 * 60e3

function isBrowserClosedError(error) {
	let message = String(error?.message || error || '').toLowerCase()
	return message.includes('target page') && message.includes('has been closed')
		|| message.includes('context or browser has been closed')
		|| message.includes('browser has been closed')
		|| message.includes('target closed')
		|| error?.name === 'TargetClosedError'
}

function isTimeoutError(error) {
	let message = String(error?.message || error || '').toLowerCase()
	return error?.name === 'TimeoutError' || message.includes('timeout')
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
	if (error?.code === 'CAPTCHA') return error
	if (error?.code === 'TIMEOUT') return error
	let message = String(error?.message || error || '')
	let err = new Error(`Browse failed: ${message}`)
	err.code = 'BROWSE_ERROR'
	return err
}

async function detectCaptcha(page) {
	let captchaFrame = await page.$('iframe[src*="recaptcha"], iframe[src*="hcaptcha"]')
	if (captchaFrame) return true
	let cfTurnstile = await page.$('input[name="cf-turnstile-response"], div.cf-turnstile')
	if (cfTurnstile) return true
	try {
		let html = await page.content()
		let lower = String(html || '').toLowerCase()
		if (lower.includes('captcha') && (lower.includes('recaptcha') || lower.includes('hcaptcha') || lower.includes('turnstile'))) return true
		if (lower.includes('verify you are human')) return true
		if (lower.includes('are you a robot')) return true
		if (lower.includes('press and hold')) return true
		if (lower.includes('cloudflare')) return true
	} catch {}
	return false
}

async function detectArchiveNoResults(page) {
	try {
		let text = await page.textContent('body')
		let lower = String(text || '').toLowerCase()
		if (lower.includes('no results')) return true
		if (lower.includes('no archive')) return true
		if (lower.includes('nothing found')) return true
		if (lower.includes('not in archive')) return true
	} catch {}
	return false
}

export async function browseArticle(url, { ignoreCooldown = false } = {}) {
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

		let skipArchive = false
		if (await detectCaptcha(page)) {
			log('[warn] captcha detected on archive; skipping archive')
			skipArchive = true
		} else if (await detectArchiveNoResults(page)) {
			log('[warn] archive has no results; skipping archive')
			skipArchive = true
		} else {
			log('no captcha detected')
		}

		if (!skipArchive) {
			const versions = await page.$$('.TEXT-BLOCK > a')
			if (versions.length > 0) {
				log('going to the newest version...')
				await versions[0].click()
				await page.waitForLoadState('load')
			}
		}

		let html = skipArchive ? '' : await page.evaluate(() => {
			return [...document.querySelectorAll('.body')].map(x => x.innerHTML).join('')
		})

		if (!html) {
			log('browsing source...')
			let cooldown = isDomainInCooldown(url)
			if (cooldown && !ignoreCooldown) {
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
				if (isTimeoutError(e)) {
					log('browse timeout', new URL(url).hostname.replace(/^www\./, ''), '10s')
					setDomainCooldown(url, 2 * 60e3, 'timeout')
					let err = new Error('browse timeout')
					err.code = 'TIMEOUT'
					throw err
				}
				log(e)
			}
			if (await detectCaptcha(page)) {
				log('[warn] captcha detected on source; skipping source')
				setDomainCooldown(url, captchaCooldownMs, 'captcha')
				let err = new Error('captcha detected on source')
				err.code = 'CAPTCHA'
				throw err
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
				if (isTimeoutError(e)) {
					log('browse timeout', new URL(url).hostname.replace(/^www\./, ''), '10s')
					setDomainCooldown(url, 2 * 60e3, 'timeout')
					let err = new Error('browse timeout')
					err.code = 'TIMEOUT'
					throw err
				}
				log(e)
			}
			if (await detectCaptcha(page)) {
				log('[warn] captcha detected on source; skipping source')
				setDomainCooldown(url, captchaCooldownMs, 'captcha')
				let err = new Error('captcha detected on source')
				err.code = 'CAPTCHA'
				throw err
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
