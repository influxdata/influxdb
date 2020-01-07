// Types
import {RemoteDataState, LocalStorage} from 'src/types'

export const normalizeResources = (state: LocalStorage) => {
  return {
    orgs: normalizeOrgs(state.resources.orgs),
  }
}

const normalizeOrgs = (orgs: LocalStorage['resources']['orgs']) => {
  return {
    ...orgs,
    org: orgs.org,
    status: RemoteDataState.NotStarted,
  }
}
