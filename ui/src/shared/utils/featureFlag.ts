import {FunctionComponent} from 'react'
import {store} from 'src'
import {activeFlags} from 'src/shared/selectors/flags'
import {clearOverrides, setOverride} from 'src/shared/actions/me'

export const isFlagEnabled = (flagName: string, equals?: string | boolean) => {
  let _equals = equals
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

export const getUserFlags = () => activeFlags(store.getState())

/* eslint-disable no-console */
const list = () => {
  console.log('Currently Available Feature Flags')
  console.table(getUserFlags())
}
/* eslint-enable no-console */

const reset = () => {
  store.dispatch(clearOverrides())
}

export const set = (flagName: string, value: string | boolean) => {
  store.dispatch(setOverride(flagName, value))
}

export const toggle = (flagName: string) => {
  const flags = activeFlags(store.getState())

  store.dispatch(setOverride(flagName, !flags[flagName]))
}

// Expose utility in dev tools console for convenience
const w: any = window

w.influx = {toggle, list, reset, set}
