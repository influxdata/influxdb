import {Links} from 'src/types/links'
import {Notification} from 'src/types'
import {TimeRange} from 'src/types/queries'
import {TimeMachinesState} from 'src/timeMachine/reducers'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'
import {RouterState} from 'react-router-redux'
import {MeState} from 'src/shared/reducers/me'
import {CurrentDashboardState} from 'src/shared/reducers/currentDashboard'
import {NoteEditorState} from 'src/dashboards/reducers/notes'
import {DataLoadingState} from 'src/dataLoaders/reducers'
import {OnboardingState} from 'src/onboarding/reducers'
import {PredicatesState, VariableEditorState} from 'src/types'
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

import {ResourceState} from 'src/types'

export interface AppState {
  alertBuilder: AlertBuilderState
  app: AppPresentationState
  autoRefresh: AutoRefreshState
  cloud: {limits: LimitsState; demoData: DemoDataState}
  currentPage: CurrentPage
  currentDashboard: CurrentDashboardState
  dataLoading: DataLoadingState
  links: Links
  me: MeState
  noteEditor: NoteEditorState
  notifications: Notification[]
  onboarding: OnboardingState
  overlays: OverlayState
  predicates: PredicatesState
  ranges: RangeState
  resources: ResourceState
  routing: RouterState
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
