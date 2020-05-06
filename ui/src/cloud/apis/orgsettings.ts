import AJAX from 'src/utils/ajax'
import {OrgSettings} from 'src/types'

export const getOrgSettings = async (orgID: string): Promise<OrgSettings> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url: `/api/v2private/orgs/${orgID}/settings`,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
