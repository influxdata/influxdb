import {
  Authorization,
  Bucket,
  Cell,
  Check,
  Dashboard,
  Member,
  NotificationEndpoint,
  NotificationRule,
  Organization,
  RemoteDataState,
  Scraper,
  TasksState,
  Telegraf,
  TemplatesState,
  VariablesState,
  View,
  Label,
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
  Views = 'views',
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

export interface RulesState extends NormalizedState<NotificationRule> {
  current: {status: RemoteDataState; rule: NotificationRule}
}

// Cells "allIDs" are Dashboard.cells
type CellsState = Omit<NormalizedState<Cell>, 'allIDs'>

// ResourceState defines the types for normalized resources
export interface ResourceState {
  [ResourceType.Authorizations]: NormalizedState<Authorization>
  [ResourceType.Buckets]: NormalizedState<Bucket>
  [ResourceType.Cells]: CellsState
  [ResourceType.Checks]: NormalizedState<Check>
  [ResourceType.Dashboards]: NormalizedState<Dashboard>
  [ResourceType.Labels]: NormalizedState<Label>
  [ResourceType.Members]: NormalizedState<Member>
  [ResourceType.Orgs]: OrgsState
  [ResourceType.Scrapers]: NormalizedState<Scraper>
  [ResourceType.Tasks]: TasksState
  [ResourceType.Telegrafs]: TelegrafsState
  [ResourceType.Templates]: TemplatesState
  [ResourceType.Variables]: VariablesState
  [ResourceType.Views]: NormalizedState<View>
  [ResourceType.NotificationEndpoints]: NormalizedState<NotificationEndpoint>
  [ResourceType.NotificationRules]: RulesState
}
