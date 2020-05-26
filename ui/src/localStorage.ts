import {
  normalizeGetLocalStorage,
  normalizeSetLocalStorage,
} from 'src/normalizers/localStorage'
import {VERSION} from 'src/shared/constants'
import {
  newVersion,
  loadLocalSettingsFailed,
} from 'src/shared/copy/notifications'

import {LocalStorage} from 'src/types/localStorage'

export const loadLocalStorage = (): LocalStorage => {
  try {
    const serializedState = localStorage.getItem('state')
    const state = JSON.parse(serializedState) || {}

    if (state.VERSION && state.VERSION !== VERSION) {
      const version = VERSION ? ` (${VERSION})` : ''

      console.log(newVersion(version).message) // eslint-disable-line no-console
    }

    delete state.VERSION

    return normalizeGetLocalStorage(state)
  } catch (error) {
    console.error(loadLocalSettingsFailed(error).message)
  }
}

const isValidJSONString = errorString => {
  try {
    JSON.parse(errorString)
  } catch (e) {
    return false
  }
  return true
}

export const saveToLocalStorage = (state: LocalStorage): void => {
  try {
    window.localStorage.setItem(
      'state',
      JSON.stringify(normalizeSetLocalStorage(state))
    )
  } catch (error) {
    const errorMessage = isValidJSONString(error) ? JSON.parse(error) : error
    console.error('Unable to save state to local storage: ', errorMessage)
  }
}
