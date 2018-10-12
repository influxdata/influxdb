import {getOrganizations as getOrganizationsAPI} from 'src/shared/apis/v2/organization'
import {Organization} from 'src/shared/apis/v2/organization'

export enum ActionTypes {
  SetOrganizations = 'SET_ORGANIZATIONS',
}

export interface SetOrganizations {
  type: ActionTypes.SetOrganizations
  payload: {
    organizations: Organization[]
  }
}

export type Actions = SetOrganizations

export const getOrganizations = () => async (
  dispatch,
  getState
): Promise<void> => {
  try {
    const {
      links: {orgs},
    } = getState()
    const organizations = await getOrganizationsAPI(orgs)
    dispatch(setOrganizations(organizations))
  } catch (e) {
    console.error(e)
  }
}

export const setOrganizations = (
  organizations: Organization[]
): SetOrganizations => {
  return {
    type: ActionTypes.SetOrganizations,
    payload: {organizations},
  }
}
