import {Dashboard} from 'src/types'

export interface DashboardsResponse {
  dashboards: Dashboard[]
}

export type GetDashboards = () => Promise<DashboardsResponse>
export interface LoadLinksOptions {
  activeDashboard: Dashboard
  dashboardsAJAX?: GetDashboards
}
