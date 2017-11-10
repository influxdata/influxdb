import {DEFAULT_ORG} from 'src/admin/constants/dummyUsers'

export const DEFAULT_PROVIDER_MAP_ID = '0'
export const PROVIDER_MAPS = [
  {
    id: DEFAULT_PROVIDER_MAP_ID,
    scheme: '*',
    provider: '*',
    providerOrganization: '*',
    redirectOrg: {id: DEFAULT_ORG.id, name: DEFAULT_ORG.name},
  },
  {
    id: '1',
    scheme: 'oauth2',
    provider: 'github',
    providerOrganization: null,
    redirectOrg: {id: '5', name: 'moarOrg'},
  },
]
