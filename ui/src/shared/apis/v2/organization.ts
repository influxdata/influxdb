import _ from 'lodash'
import {orgsAPI} from 'src/utils/api'

import {Organization} from 'src/api'

export const getOrganizations = async (): Promise<Organization[]> => {
  const {data} = await orgsAPI.orgsGet()
  return data.orgs
}
