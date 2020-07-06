import {RouterState} from 'connected-react-router'
import {TimeMachinesState} from 'src/timeMachine/reducers'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'
import {MeState} from 'src/shared/reducers/me'
import {FlagState} from 'src/shared/reducers/flags'
import {CurrentDashboardState} from 'src/shared/reducers/currentDashboard'
import {NoteEditorState} from 'src/dashboards/reducers/notes'
import {DataLoadingState} from 'src/dataLoaders/reducers'
import {OnboardingState} from 'src/onboarding/reducers'
import {
  Links,
  Notification,
  PredicatesState,
  ResourceState,
  TimeRange,
  VariableEditorState,
} from 'src/types'
import {
  TelegrafEditorPluginState,
  PluginResourceState,
  TelegrafEditorActivePluginState,
  TelegrafEditorState,
} from 'src/dataLoaders/reducers/telegrafEditor'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {UserSettingsState} from 'src/userSettings/reducers'
import {OverlayState} from 'src/overlays/reducers/overlays'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {LimitsState} from 'src/cloud/reducers/limits'
import {AlertBuilderState} from 'src/alerting/reducers/alertBuilder'
import {CurrentPage} from 'src/shared/reducers/currentPage'
import {DemoDataState} from 'src/cloud/reducers/demodata'
import {OrgSettingsState} from 'src/cloud/reducers/orgsettings'
import {QueryCacheState} from 'src/queryCache/reducers'

export interface AppState {
  router: RouterState
  alertBuilder: AlertBuilderState
  app: AppPresentationState
  autoRefresh: AutoRefreshState
  cloud: {
    limits: LimitsState
    demoData: DemoDataState
    orgSettings: OrgSettingsState
  }
  currentPage: CurrentPage
  currentDashboard: CurrentDashboardState
  queryCache: QueryCacheState
  dataLoading: DataLoadingState
  links: Links
  me: MeState
  flags: FlagState
  noteEditor: NoteEditorState
  notifications: Notification[]
  onboarding: OnboardingState
  overlays: OverlayState
  predicates: PredicatesState
  ranges: RangeState
  resources: ResourceState
  telegrafEditorPlugins: TelegrafEditorPluginState
  telegrafEditorActivePlugins: TelegrafEditorActivePluginState
  plugins: PluginResourceState
  telegrafEditor: TelegrafEditorState
  timeMachines: TimeMachinesState
  timeRange: TimeRange
  userSettings: UserSettingsState
  variableEditor: VariableEditorState
  VERSION: string
}

export type GetState = () => AppState
