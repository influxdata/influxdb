import _ from 'lodash'
import normalizer from 'src/normalizers/dashboardTime'
import {
  newVersion,
  loadLocalSettingsFailed,
} from 'src/shared/copy/notifications'

import {LocalStorage} from 'src/types/localStorage'

declare const VERSION: string

export const loadLocalStorage = (errorsQueue: any[]): LocalStorage | {} => {
  try {
    const serializedState = localStorage.getItem('state')

    const state = JSON.parse(serializedState) || {}

    if (state.VERSION && state.VERSION !== VERSION) {
      const version = VERSION ? ` (${VERSION})` : ''

      console.log(newVersion(version).message) // tslint:disable-line no-console
      errorsQueue.push(newVersion(version))
    }

    delete state.VERSION

    return state
  } catch (error) {
    console.error(loadLocalSettingsFailed(error).message)
    errorsQueue.push(loadLocalSettingsFailed(error))

    return {}
  }
}

export const saveToLocalStorage = ({
  app: {persisted},
  dataExplorerQueryConfigs,
  timeRange,
  dataExplorer,
  ranges,
  logs,
  script,
}: LocalStorage): void => {
  try {
    const appPersisted = {app: {persisted}}
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
        ranges: normalizer(ranges),
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
