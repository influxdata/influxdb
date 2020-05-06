import {produce} from 'immer'

import {Actions, ActionTypes} from 'src/cloud/actions/orgsettings'
import {OrgSetting} from 'src/types'

import {PAID_ORG_HIDE_UPGRADE_SETTING} from 'src/cloud/constants'

export interface OrgSettingsState {
  settings: OrgSetting[]
}

export const defaultState: OrgSettingsState = {
  settings: [PAID_ORG_HIDE_UPGRADE_SETTING],
}

export const orgSettingsReducer = (
  state = defaultState,
  action: Actions
): OrgSettingsState =>
  produce(state, draftState => {
    if (action.type === ActionTypes.SetOrgSettings) {
      const {orgSettings} = action.payload
      draftState.settings = orgSettings
    }
    return
  })
