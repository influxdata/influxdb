import {AppState, TimeZone} from 'src/types'

export const timeZone = (state: AppState): TimeZone =>
  state.app.persisted.timeZone || ('Local' as TimeZone)

export const hasUpdatedTimeRangeInVEO = (state: AppState): boolean =>
  state.app.ephemeral.hasUpdatedTimeRangeInVEO || false
