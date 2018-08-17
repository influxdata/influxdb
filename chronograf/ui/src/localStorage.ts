import normalizer from 'src/normalizers/dashboardTime'
import {
  newVersion,
  loadLocalSettingsFailed,
} from 'src/shared/copy/notifications'

import {LocalStorage} from 'src/types/localStorage'

const VERSION = process.env.npm_package_version

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
  script,
}: LocalStorage): void => {
  try {
    const appPersisted = {app: {persisted}}
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
      })
    )
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err))
  }
}
