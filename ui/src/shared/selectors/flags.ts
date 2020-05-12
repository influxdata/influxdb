import {AppState} from 'src/types'
import {FlagMap} from 'src/shared/reducers/flags'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

export const OSS_FLAGS = {
  deleteWithPredicate: false,
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  demodata: false,
  fluxParser: false,
}

export const CLOUD_FLAGS = {
  deleteWithPredicate: false,
  multiUser: true,
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  demodata: false,
  fluxParser: false,
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
