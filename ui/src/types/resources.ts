import {Member, RemoteDataState} from 'src/types'

export enum ResourceType {
  Labels = 'labels',
  Buckets = 'buckets',
  Telegrafs = 'telegrafs',
  Variables = 'variables',
  Authorizations = 'tokens',
  Scrapers = 'scrapers',
  Dashboards = 'dashboards',
  Tasks = 'tasks',
  Templates = 'templates',
  Members = 'members',
  Checks = 'checks',
  NotificationRules = 'rules',
  NotificationEndpoints = 'endpoints',
}

export interface NormalizedState<R> {
  byID: {
    [uuid: string]: R
  }
  allIDs: string[]
  status: RemoteDataState
}

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [ResourceType.Members]: NormalizedState<Member>
}
