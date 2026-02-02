# Coffee Grinder

## Overview
Coffee Grinder is a news pipeline that pulls Google News feeds, resolves Google News URLs, fetches article text, verifies it with GPT, falls back to other agencies when needed, summarizes, and produces slides/screenshots/audio assets.

High-level flow (current behavior in `grinder/src/2.summarize.js`):
1. Decode URL
   - Uses `gnUrl` -> `decodeGoogleNewsUrl` with a rate limiter/cooldown.
   - Logs `decode_url` ok/fail.
2. Fetch article text (primary source)
   - Up to `fetchAttempts = 2` cycles.
   - Each cycle: `fetchArticle(url)` -> extract text -> require length > 400.
   - If no text: `browseArticle(url)` -> extract text -> require length > 400.
   - If no text after all attempts: log `no_text` and go to fallback.
3. GPT verification (depending on `verifyMode`)
   - Uses `gpt-4o` on each accepted text up to `verifyMaxChars` (default 6000).
   - Threshold: `verifyMinConfidence` (default 0.7).
   - Returns `match/confidence/reason/page_summary` (`page_summary` <= 200 chars).
   - Outcomes:
     - ok: accepted and saved
     - mismatch: log reason + page summary, go to fallback
     - GPT unavailable: if `verifyFailOpen=true`, accept and mark `unverified`
4. Fallback to other agencies in the same Google News cluster
   - Uses `event.articles` (cluster items).
   - Filters by agency trust levels (`agencyLevels`, `minAgencyLevel`, `fallbackMinAgencyLevel`).
   - For each candidate: decode -> fetch/browse -> verify.
   - mismatch: log and try next source.
   - success: save and pin source/gnUrl/url.
5. Save + summarize
   - Writes HTML + TXT to `grinder/articles/`.
   - Runs summarization; failures are logged (pipeline continues).

## Quickstart

### macOS
From `grinder/`:
```sh
npm install
npm run cleanup
npm run load
npm run summarize
npm run slides
npm run screenshots
npm run upload-img
npm run audio
```
Notes:
- Node 24 is expected (see Windows scripts and repo notes).
- Screenshots use Playwright. If browsers are missing, run:
  ```sh
  npx playwright install
  ```

### Windows
The `.bat` helpers in repo root automate the pipeline. Typical flow:
1. `0.prepare.bat` (install + cleanup)
2. `2.summarize.bat`
3. `3.output.bat` (slides + local screenshot tool + audio)
4. `auto.bat` (full pipeline in one go)

The scripts assume `fnm use 24` and Chrome installed. Adjust if your Node manager differs.

## Outputs
- `grinder/articles/*.html` and `grinder/articles/*.txt` (raw and extracted text)
- `img/*.jpg` + `img/screenshots.txt` (screenshots)
- `audio/*.mp3` (generated narration)
- `grinder/logs/*.log` (pipeline logs)

## Key config files
All config is under `grinder/config/`:
- `feeds.js` - Google News RSS topics + max items.
- `agencies.js` - `agencyLevels`, `defaultAgencyLevel`, and `restricted` (paywall/scraping list).
- `verification.js` - GPT verification settings (see below).
- `external-search.js` - external search provider and limits (see below).
- `fetch.js` - fetch fallback behavior + archive cooldowns.
- `summarize.js` - summarization flags and failure logging limits.
- `topics.js` - topic taxonomy and per-topic limits.
- `google-drive.js` - Drive/Sheets IDs, template slide IDs, folder names.
- `sheets.js` - sheet size/oversize handling.
- `logging.js` - logging verbosity and limits.

## Environment variables (.env)
The pipeline loads environment variables via `dotenv` (see npm scripts).
Common values:
- `SERVICE_ACCOUNT_EMAIL` / `SERVICE_ACCOUNT_KEY` (Google APIs)
- `GOOGLE_SHEET_ID_MAIN`, `GOOGLE_SHEET_ID_AUTO` (override defaults)
- `SEARCH_API_KEY` (external search providers)
- OpenAI and ElevenLabs API keys as required by their SDKs

## Search engines
The pipeline uses two sources:
1. Google News RSS
   - `grinder/config/feeds.js` for initial loading.
   - Google News RSS search in `2.summarize.js` for missing metadata.
2. External search providers (optional)
   - Config: `grinder/config/external-search.js`.
   - Providers available in `grinder/src/external-search.js`:
     - `serper`
     - `brave`
     - `serpapi`
   - Enable with `externalSearch.enabled = true` and set `SEARCH_API_KEY`.

## External search providers (API keys)
Use one provider at a time via `externalSearch.provider` and set `SEARCH_API_KEY`.

Where to create tokens (official pages):
- Serper signup + API keys page (requires login):  
  `https://serper.dev/signup`  
  `https://serper.dev/api-keys`
- Brave Search API product + dashboard (API keys live in the dashboard):  
  `https://brave.com/search/api/`  
  `https://api-dashboard.search.brave.com/app/keys`
- SerpAPI signup + API key page (requires login):  
  `https://serpapi.com/users/sign_up`  
  `https://serpapi.com/manage-api-key`

## Provider comparison (quick guide)
This project currently uses each provider's general web search endpoint (not a
news-only endpoint). If you want news-specific results, extend
`grinder/src/external-search.js`.

| Provider | Auth method used in this repo | Endpoint used here | When to pick | Notes |
| --- | --- | --- | --- | --- |
| Serper | `X-API-KEY` header | `https://google.serper.dev/search` | You want a simple Google SERP-style API | Pricing/limits vary by plan; check dashboard |
| Brave | `X-Subscription-Token` header | `https://api.search.brave.com/res/v1/web/search` | You want Brave Search API coverage | API key is issued in Brave's dashboard after signup |
| SerpAPI | `api_key` query param | `https://serpapi.com/search.json` (engine=google) | You want SerpAPI with Google engine | Other engines require code changes |

Selection parameters to consider:
- Cost per request and minimum monthly spend
- Rate limits and concurrency caps
- Geo/language controls and localization support
- Freshness/recency controls for news
- Result fields returned (title/snippet/source) and normalization needs
- Reliability, uptime, and dashboard observability

## Verification settings
`grinder/config/verification.js` controls GPT verification and fallback thresholds:
- `verifyMode`:
  - `always` - verify every accepted text (current default).
  - `fallback` - verify only fallback sources.
  - `short` - verify only when text length < `verifyShortThreshold`.
- `verifyMinConfidence` - minimum confidence for match (default 0.7).
- `verifyShortThreshold` - char threshold for `short` mode (default 1200).
- `verifyFailOpen` - if GPT fails, accept text and mark `unverified`.
- `verifyMaxChars` - max chars sent to GPT (default 6000).
- `verifySummaryMaxChars` - max chars for `page_summary` (default 200).
- `minAgencyLevel` - minimum trust level for primary source filtering.
- `fallbackMinAgencyLevel` - minimum trust level for fallback pool.

## verifyStatus column (Google Sheets)
`2.summarize.js` auto-creates the `verifyStatus` column in the `news` sheet.
Current values written by the pipeline:
- `ok` - GPT verified match at or above confidence threshold.
- `mismatch` - GPT says text does not match the event (triggers fallback).
- `unverified` - GPT unavailable and `verifyFailOpen=true`.
- `skipped` - verification skipped by `verifyMode` (`fallback` or `short`).
- `error` - GPT failed and `verifyFailOpen=false`.

## Key modules (reference)
- `grinder/src/0.cleanup.js` - resets artifacts and sheets.
- `grinder/src/1.load.js` - loads Google News feeds into the sheet.
- `grinder/src/2.summarize.js` - fetch, verify, fallback, summarize.
- `grinder/src/3.slides.js` - builds Google Slides deck.
- `grinder/src/screenshots.js` - renders slide screenshots (Playwright).
- `grinder/src/upload-img.js` - uploads images to Drive.
- `grinder/src/4.audio.js` - generates narration via ElevenLabs.
- `grinder/src/verify-article.js` - GPT verification logic.
- `grinder/src/fetch-article.js` - fetch + archive/Jina/Wayback fallback.
- `grinder/src/browse-article.js` - Playwright browser fallback.
- `grinder/src/google-news.js` - Google News URL decode.
- `grinder/src/external-search.js` - external search adapters.
- `grinder/src/store.js` - loads/saves the `news` table in Google Sheets.

## Notes for operators
- Summaries can fail per-item; failures are logged and the pipeline continues.
- `summarizeConfig.processVerifiedOk` lets you skip already-verified items.
- Topic taxonomy is driven by `grinder/config/topics.js` and the `ai-instructions` sheet.
- If Playwright is installed but Chrome is missing, install Chrome or update `browse-article.js` paths.
