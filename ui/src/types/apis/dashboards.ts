import {Dashboard} from 'src/types/v2/dashboards'
import {AxiosResponse} from 'axios'

export interface DashboardsResponse {
  dashboards: Dashboard[]
}

export type GetDashboards = () => Promise<AxiosResponse<DashboardsResponse>>
export interface LoadLinksOptions {
  activeDashboard: Dashboard
  dashboardsAJAX?: GetDashboards
}
