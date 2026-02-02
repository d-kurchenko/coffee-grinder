import { JSDOM } from 'jsdom'

import { log } from './log.js'

let gnCooldownUntil = 0
let gnLastStatus = 0

export function getGoogleNewsDecodeCooldownMs() {
	if (Date.now() < gnCooldownUntil) return gnCooldownUntil - Date.now()
	return 0
}

export function getGoogleNewsDecodeStatus() {
	return gnLastStatus
}

function setDecodeCooldownMs(ms) {
	gnCooldownUntil = Date.now() + ms
}

export async function decodeGoogleNewsUrl(url) {
	if (Date.now() < gnCooldownUntil) {
		log('google news decode cooldown active', Math.ceil((gnCooldownUntil - Date.now()) / 1000), 's')
		return
	}
	for (let i = 0; i < 5; i++) {
		try {
			let parsedUrl = new URL(url)
			let id = parsedUrl.pathname.split('/').pop()
			let response = await fetch(`https://news.google.com/articles/${id}`)
			if (!response.ok) {
				gnLastStatus = response.status
				log(`Fetch failed: ${response.status} ${response.statusText}`)
				if (response.status === 429) {
					setDecodeCooldownMs(10 * 60e3)
					return
				}
			}
			let html = await response.text()
			let dom = new JSDOM(html)
			let div = dom.window.document.querySelector('c-wiz > div[jscontroller]')
			let [sg, ts] = [div.getAttribute('data-n-a-sg'), div.getAttribute('data-n-a-ts')]
			let payload = [
				'Fbv4je',
				`["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"${id}",${ts},"${sg}"]`,
			]
			let batchResp = await fetch('https://news.google.com/_/DotsSplashUi/data/batchexecute', {
				method: 'POST',
				headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
				body: new URLSearchParams({ 'f.req': JSON.stringify([[payload]]) }).toString(),
			})
			if (!batchResp.ok) {
				gnLastStatus = batchResp.status
				log(`Fetch failed: ${batchResp.status} ${batchResp.statusText}`)
				if (batchResp.status === 429) {
					setDecodeCooldownMs(10 * 60e3)
					return
				}
			}
			let json = await batchResp.text()
			return JSON.parse(JSON.parse(json.split('\n\n')[1].slice(0))[0][2])[1]
		} catch(e) {
			log('Fetch failed:', e)
		}
	}
}
