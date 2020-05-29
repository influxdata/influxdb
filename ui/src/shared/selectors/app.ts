import {AppState, TimeZone} from 'src/types'

export const timeZone = (state: AppState): TimeZone =>
  state.app.persisted.timeZone || ('Local' as TimeZone)
