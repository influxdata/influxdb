import {getAPIBasepath} from 'src/utils/basepath'
import {OrgSettingsResponse} from 'src/types'

export const fetchOrgSettings = async (
  orgID: string
): Promise<OrgSettingsResponse> =>
  await fetch(`${getAPIBasepath()}/api/v2private/orgs/${orgID}/settings`)
