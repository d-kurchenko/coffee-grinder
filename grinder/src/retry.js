import { log } from './log.js'
import { sleep } from './sleep.js'

/**
 * Generic retry utility
 * @param {Function} fn - Function to retry
 * @param {Object} options - Retry options
 * @param {number} options.retries - Number of total attempts (default 3)
 * @param {number} options.delay - Initial delay between retries in ms (default 1000)
 * @param {string} options.label - Label for logging
 * @returns {Promise<any>}
 */
export async function withRetry(fn, { retries = 3, delay = 1000, label = 'Operation' } = {}) {
	let lastError
	for (let i = 0; i < retries; i++) {
		try {
			const result = await fn()
			return result
		} catch (e) {
			lastError = e
			log(`${label} failed (attempt ${i + 1}/${retries}):`, e?.message || e)
			if (i < retries - 1) {
				const currentDelay = delay * Math.pow(2, i) // Exponential backoff
				log(`Retrying in ${currentDelay}ms...`)
				await sleep(currentDelay)
			}
		}
	}
	throw lastError
}
