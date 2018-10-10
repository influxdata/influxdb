import {Source} from 'src/types/v2/sources'
import {Bucket} from 'src/types/v2/buckets'
import {VEOState} from 'src/dashboards/reducers/v2/veo'
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

// TODO(chnn): Convert configureStore.js to TypeScript, then move this
export interface AppState {
  dashboards: Dashboard[]
  veo: VEOState
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
