import {Dashboard} from 'src/types/dashboards'
import {AxiosResponse} from 'axios'

export interface DashboardsResponse {
  dashboards: Dashboard[]
}

export type GetDashboards = () => Promise<AxiosResponse<DashboardsResponse>>
