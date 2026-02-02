export const externalSearch = {
	enabled: true,
	provider: 'serpapi',
	apiKey: process.env.SEARCH_API_KEY || '',
	maxResults: 6,
	timeoutMs: 10000,
}
