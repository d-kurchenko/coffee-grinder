import { AgencyLevel } from './agencies.js'

export const verifyMode = 'always'
export const verifyMinConfidence = 0.7
export const verifyShortThreshold = 1200
export const verifyFailOpen = false
export const verifyMaxChars = 0
export const verifySummaryMaxChars = 200
export const verifyContextMaxChars = 0
export const verifyFallbackMaxChars = 12000
export const verifyFallbackContextMaxChars = 4000
export const verifyModel = 'grok-4'
export const verifyTemperature = 0
export const verifyUseSearch = true
export const agencySearchMax = 8
export const agencySearchQueryMax = 2
export const minAgencyLevel = AgencyLevel.Niche
export const fallbackMinAgencyLevel = AgencyLevel.Niche
