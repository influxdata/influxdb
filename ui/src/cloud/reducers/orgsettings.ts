import {produce} from 'immer'

import {Action, SET_ORG_SETTINGS} from 'src/cloud/actions/orgsettings'
import {OrgSetting} from 'src/types'

import {PAID_ORG_HIDE_UPGRADE_SETTING} from 'src/cloud/constants'

export interface OrgSettingsState {
  settings: OrgSetting[]
}

export const defaultState: OrgSettingsState = {
  settings: [PAID_ORG_HIDE_UPGRADE_SETTING],
}

export const orgSettingsReducer = (
  state: OrgSettingsState = defaultState,
  action: Action
): OrgSettingsState =>
  produce(state, draftState => {
    if (action.type === SET_ORG_SETTINGS) {
      draftState.settings = action.payload.orgSettings
    }
    return
  })
