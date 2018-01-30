import {DEFAULT_ORG_ID} from 'src/admin/constants/chronografAdmin'

export const DEFAULT_PROVIDER_MAP_ID = '0'
export const PROVIDER_MAPS = [
  {
    id: DEFAULT_PROVIDER_MAP_ID,
    scheme: '*',
    provider: '*',
    providerOrganization: '*',
    organizationId: DEFAULT_ORG_ID,
  },
  {
    id: '1',
    scheme: 'oauth2',
    provider: 'github',
    providerOrganization: null,
    organizationId: '2',
  },
]
