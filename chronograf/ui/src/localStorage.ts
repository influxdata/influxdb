import _ from 'lodash'
import normalizer from 'src/normalizers/dashboardTime'
import {
  notifyNewVersion,
  notifyLoadLocalSettingsFailed,
} from 'src/shared/copy/notifications'

import {LocalStorage} from 'src/types/localStorage'

declare var VERSION: string

export const loadLocalStorage = (errorsQueue: any[]): LocalStorage | {} => {
  try {
    const serializedState = localStorage.getItem('state')

    const state = JSON.parse(serializedState) || {}

    if (state.VERSION && state.VERSION !== VERSION) {
      const version = VERSION ? ` (${VERSION})` : ''

      console.log(notifyNewVersion(version).message) // tslint:disable-line no-console
      errorsQueue.push(notifyNewVersion(version))

      if (!state.dashTimeV1) {
        window.localStorage.removeItem('state')
        return {}
      }

      const ranges = normalizer(_.get(state, ['dashTimeV1', 'ranges'], []))
      const dashTimeV1 = {ranges}

      window.localStorage.setItem(
        'state',
        JSON.stringify({
          dashTimeV1,
        })
      )

      return {dashTimeV1}
    }

    delete state.VERSION

    return state
  } catch (error) {
    console.error(notifyLoadLocalSettingsFailed(error).message)
    errorsQueue.push(notifyLoadLocalSettingsFailed(error))

    return {}
  }
}

export const saveToLocalStorage = ({
  app: {persisted},
  dataExplorerQueryConfigs,
  timeRange,
  dataExplorer,
  dashTimeV1: {ranges},
  logs,
  script,
}: LocalStorage): void => {
  try {
    const appPersisted = {app: {persisted}}
    const dashTimeV1 = {ranges: normalizer(ranges)}

    const minimalLogs = _.omit(logs, [
      'tableData',
      'histogramData',
      'queryCount',
    ])

    window.localStorage.setItem(
      'state',
      JSON.stringify({
        ...appPersisted,
        VERSION,
        timeRange,
        dashTimeV1,
        dataExplorer,
        dataExplorerQueryConfigs,
        script,
        logs: {
          ...minimalLogs,
          histogramData: [],
          tableData: {},
          queryCount: 0,
          tableInfiniteData: minimalLogs.tableInfiniteData || {},
          tableTime: minimalLogs.tableTime || {},
        },
      })
    )
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err))
  }
}
