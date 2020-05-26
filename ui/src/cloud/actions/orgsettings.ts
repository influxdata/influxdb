// API
import {fetchOrgSettings} from 'src/cloud/apis/orgsettings'

// Constants
import {FREE_ORG_HIDE_UPGRADE_SETTING} from 'src/cloud/constants'

// Types
import {GetState, OrgSetting} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

export const SET_ORG_SETTINGS = 'SET_ORG_SETTINGS'

export type Action = ReturnType<typeof setOrgSettings>

export const setOrgSettings = (settings: OrgSetting[] = []) =>
  ({
    type: SET_ORG_SETTINGS,
    payload: {orgSettings: settings},
  } as const)

export const setFreeOrgSettings = () =>
  ({
    type: SET_ORG_SETTINGS,
    payload: {orgSettings: [FREE_ORG_HIDE_UPGRADE_SETTING]},
  } as const)

export const getOrgSettings = () => async (dispatch, getState: GetState) => {
  try {
    const org = getOrg(getState())

    const response = await fetchOrgSettings(org.id)

    if (response.status !== 200) {
      throw new Error(
        `Unable to get organization settings: ${response.statusText}`
      )
    }
    const result = await response.json()
    dispatch(setOrgSettings(result.settings))
  } catch (error) {
    dispatch(setFreeOrgSettings())
    console.error(error)
  }
}
