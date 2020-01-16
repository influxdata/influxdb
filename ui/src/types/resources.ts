import {
  Cell,
  Bucket,
  Dashboard,
  Authorization,
  Organization,
  Member,
  RemoteDataState,
  Telegraf,
  Scraper,
  TasksState,
  VariablesState,
} from 'src/types'

export enum ResourceType {
  Authorizations = 'tokens',
  Buckets = 'buckets',
  Cells = 'cells',
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

// Cells "allIDs" are Dashboard.cells
// All cells are loaded once the dashboard is fetched so, no need to duplicate 'status' state
// However, individual cells will have a RemoteDataState status
type CellsState = Omit<NormalizedState<Cell>, 'allIDs' | 'status'>

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [ResourceType.Authorizations]: NormalizedState<Authorization>
  [ResourceType.Buckets]: NormalizedState<Bucket>
  [ResourceType.Cells]: CellsState
  [ResourceType.Dashboards]: NormalizedState<Dashboard>
  [ResourceType.Members]: NormalizedState<Member>
  [ResourceType.Orgs]: OrgsState
  [ResourceType.Scrapers]: NormalizedState<Scraper>
  [ResourceType.Tasks]: TasksState
  [ResourceType.Telegrafs]: TelegrafsState
  [ResourceType.Variables]: VariablesState
}
