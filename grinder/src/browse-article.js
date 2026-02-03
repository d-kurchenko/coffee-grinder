import { log } from './log.js'
import { sleep } from './sleep.js'
import { isDomainInCooldown, setDomainCooldown } from './domain-cooldown.js'
import { close, detectCaptcha, getMeta, getPage } from './services/playwright.js'
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
	await close()
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
	let page = await getPage()
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
		let meta = {}

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
			try {
				meta = await getMeta({ page })
			} catch {}
		}
		return { html, meta }
	}
	catch (e) {
		throw toBrowseError(e)
	}
}
