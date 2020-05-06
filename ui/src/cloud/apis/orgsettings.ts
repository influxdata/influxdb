import {OrgSettings} from 'src/types'
import {getAPIBasepath} from 'src/utils/basepath'

export const getOrgSettings = async (orgID: string): Promise<OrgSettings> => {
  try {
    const response = await fetch(
      `${getAPIBasepath()}/api/v2private/orgs/${orgID}/settings`
    )
    const data = await response.json()
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
