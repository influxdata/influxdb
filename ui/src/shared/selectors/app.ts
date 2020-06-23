import {AppState, TimeZone} from 'src/types'

export const timeZone = (state: AppState): TimeZone =>
  state.app.persisted.timeZone || ('Local' as TimeZone)

export const isInVEOMode = (state: AppState): boolean =>
  state.app.ephemeral.inVEOMode || false

export const hasUpdatedTimeRangeInVEO = (state: AppState): boolean =>
  state.app.ephemeral.hasUpdatedTimeRangeInVEO || false
