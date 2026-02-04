import { existsSync } from 'fs'
import os from 'os'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'
import {chromium} from "playwright";

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const rootDir = resolve(__dirname, '..', '..')

let contextPromise
let pagePromise

function getChromeExecutablePath() {
	let platform = os.platform()
	if (platform === 'darwin') return '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
	if (platform === 'win32') return 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
	return '/usr/bin/google-chrome'
}

function getExtensionPath() {
	return resolve(rootDir, 'extensions', 'captcha-solver', '0.2.1_0')
}

async function initialize() {
	let chromeProfilePath = process.env.PLAYWRIGHT_PROFILE_DIR || resolve(rootDir, '.playwright-profile')
	let useSystemChrome = process.env.PLAYWRIGHT_USE_SYSTEM_CHROME === '1'
	let chromeExecutablePath = getChromeExecutablePath()
	let extensionPath = getExtensionPath()
	let hasExtension = existsSync(extensionPath)

	let args = [
		'--start-maximized',
		'--disable-crash-reporter',
	]
	if (hasExtension) {
		args.push(`--disable-extensions-except=${extensionPath}`)
		args.push(`--load-extension=${extensionPath}`)
	}

	// plugin.setServiceKey('');
	// const fingerprint = await plugin.fetch({
	// 	tags: ['Microsoft Windows', 'Chrome'],
	// });
	// plugin.useFingerprint(fingerprint);

	let context = await chromium.launchPersistentContext(chromeProfilePath, {
		headless: false,
		executablePath: useSystemChrome ? chromeExecutablePath : undefined,
		viewport: { width: 1024, height: 600 },
		screen: { width: 1024, height: 600 },
		args,
	})

	let page = await context.newPage()
	return { context, page }
}

async function ensureContext() {
	if (!contextPromise) contextPromise = initialize()
	return contextPromise
}

export async function getPage() {
	if (!pagePromise) {
		let { page } = await ensureContext()
		pagePromise = page
	}
	return pagePromise
}

export async function openPage(url, options = {}) {
	let page = await getPage()
	await page.goto(url, {
		waitUntil: options.waitUntil || 'load',
		timeout: options.timeout ?? 10e3,
	})
	return page
}

export async function getText(options = {}) {
	let page = await getPage()
	let selector = options.selector || 'body'
	let mode = options.mode || 'innerText'
	if (mode === 'textContent') {
		return await page.textContent(selector)
	}
	return await page.evaluate((sel) => {
		let el = document.querySelector(sel)
		return el ? el.innerText : ''
	}, selector)
}

export async function getHtml(options = {}) {
	let page = await getPage()
	let selector = options.selector || 'body'
	if (selector === 'body') return await page.content()
	return await page.evaluate((sel) => {
		let el = document.querySelector(sel)
		return el ? el.innerHTML : ''
	}, selector)
}

export async function getMeta(options = {}) {
	let page = options.page || await getPage()
	let meta = await page.evaluate(() => {
		let readMeta = (selectors) => {
			for (let selector of selectors) {
				let node = document.querySelector(selector)
				if (!node) continue
				let content = node.getAttribute('content') || node.getAttribute('value') || ''
				if (content) return content.trim()
			}
			return ''
		}
		let readLink = (selector) => {
			let node = document.querySelector(selector)
			let href = node?.getAttribute('href') || ''
			return href.trim()
		}
		let readLdJson = () => {
			let scripts = [...document.querySelectorAll('script[type=\"application/ld+json\"]')]
			if (!scripts.length) return {}
			let result = {}
			let collect = (node) => {
				if (!node || typeof node !== 'object') return
				if (Array.isArray(node)) {
					node.forEach(collect)
					return
				}
				let headline = node.headline || node.name
				let description = node.description
				let keywords = node.keywords
				let datePublished = node.datePublished || node.dateCreated || node.dateModified
				let author = node.author?.name || node.author
				let image = node.image?.url || node.image
				let mainEntity = node.mainEntityOfPage
				if (headline && !result.title) result.title = String(headline).trim()
				if (description && !result.description) result.description = String(description).trim()
				if (keywords && !result.keywords) result.keywords = Array.isArray(keywords) ? keywords.join(', ') : String(keywords)
				if (datePublished && !result.date) result.date = String(datePublished).trim()
				if (author && !result.author) result.author = String(author).trim()
				if (image && !result.image) result.image = Array.isArray(image) ? String(image[0]) : String(image)
				if (mainEntity && !result.canonicalUrl) {
					let url = mainEntity['@id'] || mainEntity.url
					if (url) result.canonicalUrl = String(url).trim()
				}
				for (let value of Object.values(node)) collect(value)
			}
			for (let script of scripts) {
				let raw = script.textContent?.trim()
				if (!raw) continue
				try {
					collect(JSON.parse(raw))
				} catch {}
			}
			return result
		}

		let ld = readLdJson()
		let title = ld.title || readMeta([
			'meta[property=\"og:title\"]',
			'meta[name=\"twitter:title\"]',
			'meta[name=\"title\"]',
		]) || (document.title ? document.title.trim() : '')
		let description = ld.description || readMeta([
			'meta[property=\"og:description\"]',
			'meta[name=\"description\"]',
			'meta[name=\"twitter:description\"]',
		])
		let keywords = ld.keywords || readMeta([
			'meta[name=\"keywords\"]',
			'meta[name=\"news_keywords\"]',
		])
		let date = ld.date || readMeta([
			'meta[property=\"article:published_time\"]',
			'meta[name=\"pubdate\"]',
			'meta[name=\"publishdate\"]',
			'meta[name=\"date\"]',
			'meta[property=\"og:updated_time\"]',
		])
		let canonicalUrl = ld.canonicalUrl || readLink('link[rel=\"canonical\"]') || readMeta([
			'meta[property=\"og:url\"]',
		])
		let image = ld.image || readMeta([
			'meta[property=\"og:image\"]',
			'meta[property=\"og:image:url\"]',
			'meta[name=\"twitter:image\"]',
		])
		let author = ld.author || readMeta([
			'meta[name=\"author\"]',
			'meta[property=\"article:author\"]',
		])
		let siteName = readMeta([
			'meta[property=\"og:site_name\"]',
		])
		let section = readMeta([
			'meta[property=\"article:section\"]',
		])
		let type = readMeta([
			'meta[property=\"og:type\"]',
		])
		let lang = document.documentElement?.getAttribute('lang') || ''

		return {
			title: title || '',
			description: description || '',
			keywords: keywords || '',
			date: date || '',
			canonicalUrl: canonicalUrl || '',
			image: image || '',
			author: author || '',
			siteName: siteName || '',
			section: section || '',
			type: type || '',
			lang: lang || '',
		}
	})
	return meta
}

export async function getMetadata(options = {}) {
	return await getMeta(options)
}

export async function screenshot(path, options = {}) {
	let page = await getPage()
	return await page.screenshot({
		path,
		fullPage: options.fullPage ?? true,
		...options,
	})
}

export async function detectCaptcha(page) {
	let targetPage = page || await getPage()
	let captchaFrame = await targetPage.$('iframe[src*="recaptcha"], iframe[src*="hcaptcha"]')
	if (captchaFrame) return true
	let cfTurnstile = await targetPage.$('input[name="cf-turnstile-response"], div.cf-turnstile')
	if (cfTurnstile) return true
	try {
		let html = await targetPage.content()
		let lower = String(html || '').toLowerCase()
		if (lower.includes('captcha') && (lower.includes('recaptcha') || lower.includes('hcaptcha') || lower.includes('turnstile'))) return true
		if (lower.includes('verify you are human')) return true
		if (lower.includes('are you a robot')) return true
		if (lower.includes('press and hold')) return true
		if (lower.includes('cloudflare')) return true
	} catch {}
	return false
}

const pageStatePatterns = {
	js_required: [
		'enable javascript',
		'javascript is required',
		'requires javascript',
		'please enable javascript',
		'please enable cookies',
	],
	blocked: [
		'access denied',
		'request blocked',
		'forbidden',
		'service unavailable',
		'unusual traffic',
		'automated requests',
		'temporarily blocked',
	],
	paywall: [
		'subscribe to continue',
		'subscribe to read',
		'subscription required',
		'subscriber-only',
		'sign in to continue',
		'log in to continue',
		'please subscribe',
		'paywall',
		'metered',
	],
	consent: [
		'cookie consent',
		'cookie preferences',
		'privacy settings',
		'gdpr',
		'accept cookies',
		'accept all cookies',
		'manage cookies',
		'cookie policy',
	],
}

export function classifyHtmlState(html = '', title = '') {
	let sample = `${title || ''}\n${html || ''}`.toLowerCase()
	if (!sample.trim()) return { state: 'empty', reason: 'empty' }
	for (let [state, patterns] of Object.entries(pageStatePatterns)) {
		for (let pattern of patterns) {
			if (sample.includes(pattern)) {
				return { state, reason: pattern }
			}
		}
	}
	return { state: 'unknown', reason: '' }
}

export async function detectPageState(page) {
	let targetPage = page || await getPage()
	if (await detectCaptcha(targetPage)) return { state: 'captcha', reason: 'captcha' }
	let html = ''
	let title = ''
	try {
		html = await targetPage.content()
	} catch {}
	try {
		title = await targetPage.title()
	} catch {}
	return classifyHtmlState(html, title)
}

export async function close() {
	if (!contextPromise) return
	let { context } = await contextPromise
	await context?.close?.()
	contextPromise = undefined
	pagePromise = undefined
}
