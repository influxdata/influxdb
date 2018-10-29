import _ from 'lodash'
import AJAX from 'src/utils/ajax'

import {Organization} from 'src/types/v2'

export const getOrganizations = async (
  url: string
): Promise<Organization[]> => {
  const {data} = await AJAX({
    url,
  })

  return _.get(data, 'orgs', [])
}
