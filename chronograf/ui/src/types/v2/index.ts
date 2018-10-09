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
import {TimeRange} from 'src/types/queries'
import {TimeMachinesState} from 'src/shared/reducers/v2/timeMachines'

// TODO(chnn): Convert configureStore.js to TypeScript, then move this. Also
// fill out with remaining state interfaces.
export interface AppState {
  dashboards: Dashboard[]
  timeMachines: TimeMachinesState
  ranges: RangeState
  views: ViewsState
  hoverTime: HoverTimeState
  links: Links
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
