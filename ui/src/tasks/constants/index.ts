import {Organization} from 'src/types/v2'

export const allOrganizationsID = 'All Organizations'

export const defaultAllOrgs: Organization = {
  id: allOrganizationsID,
  name: 'All Organizations',
  links: {
    buckets: '',
    dashboards: '',
    self: '',
    tasks: '',
  },
}
