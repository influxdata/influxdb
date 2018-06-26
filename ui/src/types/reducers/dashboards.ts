import {Dashboard} from 'src/types'
import {Me} from 'src/types/auth'
import {DashTimeV1Range} from 'src/types/queries'

export interface Dashboards {
  dashboardUI: {dashboards: Dashboard[]}
}

export interface Auth {
  auth: {isUsingAuth: boolean; me: Me}
}

export interface DashTimeV1 {
  dashTimeV1: {ranges: DashTimeV1Range[]}
}
