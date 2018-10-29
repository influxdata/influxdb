import {Source} from 'src/types/v2/sources'
import {Bucket, RetentionRule, RetentionRuleTypes} from 'src/types/v2/buckets'
import {RangeState} from 'src/dashboards/reducers/v2/ranges'
import {ViewsState} from 'src/dashboards/reducers/v2/views'
import {HoverTimeState} from 'src/dashboards/reducers/v2/hoverTime'
import {
  Dashboard,
  DashboardSwitcherLinks,
  Cell,
  View,
  ViewType,
  ViewShape,
  ViewParams,
  ViewProperties,
  DashboardQuery,
} from 'src/types/v2/dashboards'
import {Task} from 'src/types/v2/tasks'
import {Member} from 'src/types/v2/members'
import {Organization} from 'src/types/v2/orgs'
import {Links} from 'src/types/v2/links'
import {Notification} from 'src/types'
import {TimeRange} from 'src/types/queries'
import {LogsState} from 'src/types/logs'
import {User, UserToken} from 'src/types/v2/user'
import {TimeMachinesState} from 'src/shared/reducers/v2/timeMachines'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'
import {State as TaskState} from 'src/tasks/reducers/v2'
import {RouterState} from 'react-router-redux'
import {MeState} from 'src/shared/reducers/v2/me'
import {SourceState} from 'src/shared/reducers/v2/source'
import {OverlayState} from 'src/types/v2/overlay'

export interface AppState {
  VERSION: string
  links: Links
  app: AppPresentationState
  logs: LogsState
  ranges: RangeState
  views: ViewsState
  sources: Source[]
  dashboards: Dashboard[]
  hoverTime: HoverTimeState
  notifications: Notification[]
  timeMachines: TimeMachinesState
  routing: RouterState
  tasks: TaskState
  timeRange: TimeRange
  orgs: Organization[]
  me: MeState
  source: SourceState
}

export {
  User,
  UserToken,
  Source,
  Member,
  Bucket,
  OverlayState,
  RetentionRule,
  RetentionRuleTypes,
  Dashboard,
  Links,
  Cell,
  DashboardQuery,
  View,
  ViewType,
  ViewShape,
  ViewParams,
  ViewProperties,
  TimeRange,
  DashboardSwitcherLinks,
  Organization,
  Task,
}
