export const loadLocalStorage = () => {
  try {
    const serializedState = localStorage.getItem('state')

    const state = JSON.parse(serializedState)

    // eslint-disable-next-line no-undef
    if (state.VERSION !== VERSION) {
      // eslint-disable-next-line no-console
      console.log('New version detected. Clearing old settings.')
      window.localStorage.removeItem('state')
      return {}
    }

    delete state.VERSION

    return state || {}
  } catch (err) {
    // eslint-disable-line no-console
    console.error(`Loading persisted state failed: ${err}`)
    return {}
  }
}

export const saveToLocalStorage = ({
  app: {persisted},
  queryConfigs,
  timeRange,
  dataExplorer,
}) => {
  try {
    const appPersisted = Object.assign({}, {app: {persisted}})

    window.localStorage.setItem(
      'state',
      JSON.stringify({
        ...appPersisted,
        queryConfigs,
        timeRange,
        dataExplorer,
        VERSION, // eslint-disable-line no-undef
      })
    )
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err)) // eslint-disable-line no-console
  }
}
