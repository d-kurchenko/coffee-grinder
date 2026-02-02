const enableFullLog = process.argv.includes('--log-full') || process.env.LOG_FULL === '1'

export const logging = {
	duplicate: false,
	useStderr: false,
	fetchJson: enableFullLog ? true : false,
	fetchLogFile: 'logs/fetch.jsonl',
	fetchLogMaxBytes: enableFullLog ? 0 : 5 * 1024 * 1024,
	fetchLogMaxFiles: 5,
	maxStringLength: enableFullLog ? 0 : 800,
	maxDataStringLength: enableFullLog ? 0 : 800,
	includeContentText: enableFullLog ? true : false,
	contentTextMaxChars: enableFullLog ? 0 : 800,
	includeVerifyPrompt: enableFullLog ? true : false,
	verifyPromptMaxChars: enableFullLog ? 0 : 2000,
	includeAiPrompt: enableFullLog ? true : false,
	aiPromptMaxChars: enableFullLog ? 0 : 2000,
}
