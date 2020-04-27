import {FunctionComponent} from 'react'
import {activeFlags} from 'src/shared/selectors/flags'
import {clearOverrides, setOverride} from 'src/shared/actions/flags'

import configureStore from 'src/store/configureStore'

export const isFlagEnabled = (flagName: string, equals?: string | boolean) => {
  let _equals = equals
  const store = configureStore()
  const flags = activeFlags(store.getState())

  if (_equals === undefined) {
    _equals = true
  }

  if (flags.hasOwnProperty(flagName)) {
    return flags[flagName] === _equals
  }

  return false
}

// type influx.toggle('myFlag') to disable / enable any feature flag
export const FeatureFlag: FunctionComponent<{
  name: string
  equals?: string | boolean
}> = ({name, equals, children}) => {
  if (!isFlagEnabled(name, equals)) {
    return null
  }

  return children as any
}

export const getUserFlags = () => activeFlags(configureStore().getState())

/* eslint-disable no-console */
const list = () => {
  console.log('Currently Available Feature Flags')
  console.table(getUserFlags())
}
/* eslint-enable no-console */

const reset = () => {
  const store = configureStore()
  store.dispatch(clearOverrides())
}

export const set = (flagName: string, value: string | boolean) => {
  const store = configureStore()
  store.dispatch(setOverride(flagName, value))
}

export const toggle = (flagName: string) => {
  const flags = getUserFlags()

  set(flagName, !flags[flagName])
}

// Expose utility in dev tools console for convenience
const w: any = window

w.influx = {toggle, list, reset, set}
