import { logFetch } from '../fetch-log.js'
import { titleFor } from './utils.js'

export function logEvent(event, data, message, level) {
	if (event && data) {
		if (data.phase) event._lastPhase = data.phase
		if (data.status) event._lastStatus = data.status
		if (data.method) event._lastMethod = data.method
		if (data.reason) event._lastReason = data.reason
		if (data.pageSummary) event._lastPageSummary = data.pageSummary
	}
	logFetch({
		eventId: event?.id,
		title: titleFor(event),
		source: event?.source,
		gnUrl: event?.gnUrl,
		url: event?.url,
		...data,
	}, message, level)
}
