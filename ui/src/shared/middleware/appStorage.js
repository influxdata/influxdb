// makeAppStorage responds to actions that need to persist a value in localStorage.
// There are some tradeoffs between using localStorage vs. using on-disk files via electron.
// localStorage was chosen, for now, to simplify use of this app outside of electron.
export default function makeAppStorage(localStorage) {
  return () => {
    // eslint-disable-line no-unused-vars
    return next => {
      return action => {
        if (action.meta && action.meta.appStorage) {
          const stuffToStore = action.meta.appStorage
          if (Array.isArray(stuffToStore)) {
            stuffToStore.forEach(updateInLocalStorage)
          } else {
            updateInLocalStorage(stuffToStore)
          }
        }

        next(action)
      }
    }
  }

  function updateInLocalStorage({setIn, removeIn, key, value}) {
    const item = setIn || removeIn
    const existingString = localStorage.getItem(item)
    const existingJSON = JSON.parse(existingString || '{}')

    if (setIn) {
      existingJSON[key] = value
    }

    if (removeIn) {
      delete existingJSON[key]
    }

    localStorage.setItem(item, JSON.stringify(existingJSON))
  }
}
