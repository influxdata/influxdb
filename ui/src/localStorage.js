import _ from 'lodash'
import normalizer from 'src/normalizers/dashboardTime'
import {
  notifyNewVersion,
  notifyLoadLocalSettingsFailed,
} from 'src/shared/copy/notifications'

export const loadLocalStorage = errorsQueue => {
  try {
    const serializedState = localStorage.getItem('state')

    const state = JSON.parse(serializedState) || {}

    // eslint-disable-next-line no-undef
    if (state.VERSION && state.VERSION !== VERSION) {
      // eslint-disable-next-line no-undef
      const version = VERSION ? ` (${VERSION})` : ''

      console.log(notifyNewVersion(version).message) // eslint-disable-line no-console
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
    console.error(notifyLoadLocalSettingsFailed(error).message) // eslint-disable-line no-console
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
}) => {
  try {
    const appPersisted = Object.assign({}, {app: {persisted}})
    const dashTimeV1 = {ranges: normalizer(ranges)}

    window.localStorage.setItem(
      'state',
      JSON.stringify({
        ...appPersisted,
        dataExplorerQueryConfigs,
        timeRange,
        dataExplorer,
        VERSION, // eslint-disable-line no-undef
        dashTimeV1,
      })
    )
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err)) // eslint-disable-line no-console
  }
}
