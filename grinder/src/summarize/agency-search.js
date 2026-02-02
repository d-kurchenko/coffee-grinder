import { agencyDomains } from '../../config/agencies.js'
import { agencySearchMax, agencySearchQueryMax } from '../../config/verification.js'
import { searchGoogleNews } from './gn.js'
import { buildFallbackSearchQueries } from './gn.js'
import { getAgencyLevel, normalizeSource } from './utils.js'

function buildAgencyTargets() {
	let targets = []
	for (let [name, domains] of Object.entries(agencyDomains || {})) {
		let level = getAgencyLevel(name)
		let normalized = normalizeSource(name)
		let list = Array.isArray(domains) ? domains : [domains]
		if (!list.length) continue
		targets.push({ name, level, normalized, domains: list })
	}
	return targets.sort((a, b) => b.level - a.level)
}

export async function searchAgencyArticles(event, last) {
	let queries = buildFallbackSearchQueries(event)
	if (!queries.length) return []
	let limitedQueries = queries.slice(0, Math.max(1, agencySearchQueryMax || 1))
	let targets = buildAgencyTargets().slice(0, Math.max(1, agencySearchMax || 1))
	let results = []
	for (let target of targets) {
		for (let domain of target.domains) {
			for (let query of limitedQueries) {
				let searchQuery = `site:${domain} ${query}`
				let items = await searchGoogleNews(searchQuery, last)
				if (!items.length) continue
				results.push(...items.map(item => ({
					...item,
					source: item.source || target.name,
				})))
			}
		}
	}
	return results
}
