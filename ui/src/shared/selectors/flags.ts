import {AppState} from 'src/types'
import {MeFlags} from 'src/shared/reducers/me'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

export const OSS_FLAGS = {
  deleteWithPredicate: false,
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  regionBasedLoginPage: false,
  demodata: false,
}

export const CLOUD_FLAGS = {
  deleteWithPredicate: false,
  multiUser: false,
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  regionBasedLoginPage: false,
  demodata: false,
}

export const activeFlags = (state: AppState): MeFlags => {
  const localState = CLOUD ? CLOUD_FLAGS : OSS_FLAGS
  const networkState = state.me.flags || {}
  const overrides = state.me.flagOverrides || {}

  return {
    ...localState,
    ...networkState,
    ...overrides,
  }
}
