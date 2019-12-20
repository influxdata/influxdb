import {Links} from 'src/types/links'
import {Notification} from 'src/types'
import {TimeRange} from 'src/types/queries'
import {TimeMachinesState} from 'src/timeMachine/reducers'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'
import {TasksState} from 'src/tasks/reducers'
import {RouterState} from 'react-router-redux'
import {MeState} from 'src/shared/reducers/me'
import {NoteEditorState} from 'src/dashboards/reducers/notes'
import {DataLoadingState} from 'src/dataLoaders/reducers'
import {OnboardingState} from 'src/onboarding/reducers'
import {PredicatesState} from 'src/types'
import {VariablesState, VariableEditorState} from 'src/variables/reducers'
import {LabelsState} from 'src/labels/reducers'
import {BucketsState} from 'src/buckets/reducers'
import {
  TelegrafEditorPluginState,
  PluginResourceState,
  TelegrafEditorActivePluginState,
  TelegrafEditorState,
} from 'src/dataLoaders/reducers/telegrafEditor'
import {TelegrafsState} from 'src/telegrafs/reducers'
import {TemplatesState} from 'src/templates/reducers'
import {AuthorizationsState} from 'src/authorizations/reducers'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {ViewsState} from 'src/dashboards/reducers/views'
import {ScrapersState} from 'src/scrapers/reducers'
import {UserSettingsState} from 'src/userSettings/reducers'
import {DashboardsState} from 'src/dashboards/reducers/dashboards'
import {OrgsState} from 'src/organizations/reducers/orgs'
import {OverlayState} from 'src/overlays/reducers/overlays'
import {MembersState} from 'src/members/reducers'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {LimitsState} from 'src/cloud/reducers/limits'
import {ChecksState} from 'src/alerting/reducers/checks'
import {NotificationRulesState} from 'src/alerting/reducers/notifications/rules'
import {NotificationEndpointsState} from 'src/alerting/reducers/notifications/endpoints'

export interface AppState {
  app: AppPresentationState
  autoRefresh: AutoRefreshState
  buckets: BucketsState
  checks: ChecksState
  cloud: {limits: LimitsState}
  dashboards: DashboardsState
  dataLoading: DataLoadingState
  endpoints: NotificationEndpointsState
  labels: LabelsState
  links: Links
  me: MeState
  members: MembersState
  noteEditor: NoteEditorState
  notifications: Notification[]
  onboarding: OnboardingState
  orgs: OrgsState
  overlays: OverlayState
  predicates: PredicatesState
  ranges: RangeState
  routing: RouterState
  rules: NotificationRulesState
  scrapers: ScrapersState
  tasks: TasksState
  telegrafEditorPlugins: TelegrafEditorPluginState
  telegrafEditorActivePlugins: TelegrafEditorActivePluginState
  plugins: PluginResourceState
  telegrafEditor: TelegrafEditorState
  telegrafs: TelegrafsState
  templates: TemplatesState
  timeMachines: TimeMachinesState
  timeRange: TimeRange
  tokens: AuthorizationsState
  userSettings: UserSettingsState
  variables: VariablesState
  variableEditor: VariableEditorState
  VERSION: string
  views: ViewsState
}

export type GetState = () => AppState
