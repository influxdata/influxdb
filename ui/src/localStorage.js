import normalizer from 'src/normalizers/dashboardTime'

export const loadLocalStorage = errorsQueue => {
  try {
    const serializedState = localStorage.getItem('state')

    const state = JSON.parse(serializedState) || {}

    // eslint-disable-next-line no-undef
    if (state.VERSION && state.VERSION !== VERSION) {
      const errorText =
        'New version of Chronograf detected. Local settings cleared.'

      console.log(errorText) // eslint-disable-line no-console
      errorsQueue.push(errorText)

      if (!state.dashTimeV1) {
        window.localStorage.removeItem('state')
        return {}
      }

      const ranges = normalizer(state.dashTimeV1.ranges)
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
    const errorText = `Loading local settings failed: ${error}`

    console.error(errorText) // eslint-disable-line no-console
    errorsQueue.push(errorText)

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
