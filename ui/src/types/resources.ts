import {
  Bucket,
  Authorization,
  Organization,
  Member,
  RemoteDataState,
  Telegraf,
  Scraper,
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

const {
  Authorizations,
  Buckets,
  Members,
  Orgs,
  Telegrafs,
  Scrapers,
} = ResourceType

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [Authorizations]: NormalizedState<Authorization>
  [Buckets]: NormalizedState<Bucket>
  [Members]: NormalizedState<Member>
  [Orgs]: OrgsState
  [Telegrafs]: TelegrafsState
  [Scrapers]: NormalizedState<Scraper>
}
