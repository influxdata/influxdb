import {Organization, Member, RemoteDataState} from 'src/types'

export enum ResourceType {
  Buckets = 'buckets',
  Orgs = 'orgs',
  Labels = 'labels',
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
  Plugins = 'plugins',
}

export interface NormalizedState<R> {
  byID: {
    [uuid: string]: R
  }
  allIDs: string[]
  status: RemoteDataState
}

export interface OrgsState extends NormalizedState<Organization> {
  org: Organization
}

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [ResourceType.Members]: NormalizedState<Member>
  [ResourceType.Orgs]: OrgsState
}
