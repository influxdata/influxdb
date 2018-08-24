import {Source} from 'src/types/v2'
import {SourceLinks, SourceAuthenticationMethod} from 'src/types/v2/sources'

export const sourceLinks: SourceLinks = {
  query: '/v2/sources/16/query',
  self: '/v2/sources/16',
  health: '/v2/sources/16/health',
  buckets: '/v2/sources/16/buckets',
}
export const source: Source = {
  id: '16',
  name: 'ssl',
  type: 'influx',
  username: 'admin',
  url: 'https://localhost:9086',
  insecureSkipVerify: true,
  default: false,
  telegraf: 'telegraf',
  links: sourceLinks,
  authentication: SourceAuthenticationMethod.Basic,
}
