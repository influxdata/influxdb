import {AppState} from 'src/types'
import {FlagMap} from 'src/shared/reducers/flags'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

export const OSS_FLAGS = {
  cursorAtEOF: false,
  customCheckQuery: false,
  deleteWithPredicate: false,
  demodata: false,
  downloadCellCSV: false,
  fluxParser: false,
  matchingNotificationRules: false,
  notebooks: false,
  telegrafEditor: false,
}

export const CLOUD_FLAGS = {
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
  cursorAtEOF: false,
  customCheckQuery: false,
  deleteWithPredicate: false,
  demodata: true,
  downloadCellCSV: false,
  fluxParser: false,
  matchingNotificationRules: false,
  notebooks: false,
  telegrafEditor: false,
}

export const activeFlags = (state: AppState): FlagMap => {
  const localState = CLOUD ? CLOUD_FLAGS : OSS_FLAGS
  const networkState = state.flags.original || {}
  const override = state.flags.override || {}

  return {
    ...localState,
    ...networkState,
    ...override,
  }
}
