import _ from 'lodash'
import AJAX from 'src/utils/ajax'

export interface Organization {
  name: string
  id: string
}

export const getOrganizations = async (
  url: string
): Promise<Organization[]> => {
  const {data} = await AJAX({
    url,
  })

  return _.get(data, 'orgs', [])
}
