// Types
import {LocalStorage} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

export const normalizeOrgs = (orgs: LocalStorage['orgs']) => {
  return {
    ...orgs,
    status: RemoteDataState.NotStarted,
    org: orgs.org,
  }
}
