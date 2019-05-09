import {AutoRefreshStatus} from 'src/types'

export type Action = SetAutoRefresh | SetAutoRefreshStatus

interface SetAutoRefresh {
  type: 'SET_AUTO_REFRESH_INTERVAL'
  payload: {dashboardID: string; milliseconds: number}
}

export const setAutoRefreshInterval = (
  dashboardID: string,
  milliseconds: number
): SetAutoRefresh => ({
  type: 'SET_AUTO_REFRESH_INTERVAL',
  payload: {dashboardID, milliseconds},
})

interface SetAutoRefreshStatus {
  type: 'SET_AUTO_REFRESH_STATUS'
  payload: {dashboardID: string; status: AutoRefreshStatus}
}

export const setAutoRefreshStatus = (
  dashboardID: string,
  status: AutoRefreshStatus
): SetAutoRefreshStatus => ({
  type: 'SET_AUTO_REFRESH_STATUS',
  payload: {dashboardID, status},
})
