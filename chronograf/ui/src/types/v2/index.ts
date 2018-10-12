import {Source} from 'src/types/v2/sources'
import {Bucket} from 'src/types/v2/buckets'
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
  DashboardQuery,
} from 'src/types/v2/dashboards'
import {Links} from 'src/types/v2/links'
import {Notification} from 'src/types'
import {TimeRange} from 'src/types/queries'
import {LogsState} from 'src/types/logs'
import {TimeMachinesState} from 'src/shared/reducers/v2/timeMachines'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'
import {State as TaskState} from 'src/tasks/reducers/v2'
import {RouterState} from 'react-router-redux'
import {MeState} from 'src/shared/reducers/v2/me'

interface Organization {
  id: string
  name: string
}

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
}

export {
  Source,
  Bucket,
  Dashboard,
  Links,
  Cell,
  DashboardQuery,
  View,
  ViewType,
  ViewShape,
  ViewParams,
  TimeRange,
  DashboardSwitcherLinks,
}
