import {
  Bucket,
  Authorization,
  Organization,
  Member,
  RemoteDataState,
  Telegraf,
  Scraper,
  TasksState,
} from 'src/types'

export enum ResourceType {
  Authorizations = 'tokens',
  Buckets = 'buckets',
  Checks = 'checks',
  Dashboards = 'dashboards',
  Labels = 'labels',
  Orgs = 'orgs',
  Members = 'members',
  NotificationRules = 'rules',
  NotificationEndpoints = 'endpoints',
  Plugins = 'plugins',
  Scrapers = 'scrapers',
  Tasks = 'tasks',
  Templates = 'templates',
  Telegrafs = 'telegrafs',
  Variables = 'variables',
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

export interface TelegrafsState extends NormalizedState<Telegraf> {
  currentConfig: {status: RemoteDataState; item: string}
}

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [ResourceType.Authorizations]: NormalizedState<Authorization>
  [ResourceType.Buckets]: NormalizedState<Bucket>
  [ResourceType.Members]: NormalizedState<Member>
  [ResourceType.Orgs]: OrgsState
  [ResourceType.Telegrafs]: TelegrafsState
  [ResourceType.Scrapers]: NormalizedState<Scraper>
  [ResourceType.Tasks]: TasksState
}
