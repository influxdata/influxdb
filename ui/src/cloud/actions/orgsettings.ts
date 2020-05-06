// API
import {getOrgSettings as getOrgSettingsAJAX} from 'src/cloud/apis/orgsettings'

// Constants
import {FREE_ORG_HIDE_UPGRADE_SETTING} from 'src/cloud/constants'

// Types
import {GetState, OrgSetting} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

export enum ActionTypes {
  SetOrgSettings = 'SET_ORG_SETTINGS',
}

export type Actions = SetOrgSettings

export interface SetOrgSettings {
  type: ActionTypes.SetOrgSettings
  payload: {orgSettings: OrgSetting[]}
}

export const setOrgSettings = (settings: OrgSetting[]): SetOrgSettings => {
  return {
    type: ActionTypes.SetOrgSettings,
    payload: {orgSettings: settings},
  }
}

export const setFreeOrgSettings = (): SetOrgSettings => {
  return {
    type: ActionTypes.SetOrgSettings,
    payload: {orgSettings: [FREE_ORG_HIDE_UPGRADE_SETTING]},
  }
}

export const getOrgSettings = () => async (dispatch, getState: GetState) => {
  try {
    const org = getOrg(getState())

    const result = await getOrgSettingsAJAX(org.id)
    dispatch(setOrgSettings(result.settings))
  } catch (error) {
    dispatch(setFreeOrgSettings())
    console.error(error)
  }
}
