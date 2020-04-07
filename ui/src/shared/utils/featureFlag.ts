import {FunctionComponent} from 'react'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

export const OSS_FLAGS = {
  deleteWithPredicate: false,
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  regionBasedLoginPage: false,
  demodata: false,
}

export const CLOUD_FLAGS = {
  deleteWithPredicate: false,
  multiUser: false,
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
  downloadCellCSV: false,
  telegrafEditor: false,
  customCheckQuery: false,
  matchingNotificationRules: false,
  regionBasedLoginPage: false,
  demodata: false,
}

export const isFlagEnabled = (flagName: string, equals?: string | boolean) => {
  let localStorageFlags
  let _equals = equals

  try {
    localStorageFlags = JSON.parse(window.localStorage.featureFlags)
  } catch {
    localStorageFlags = {}
  }

  if (_equals === undefined) {
    _equals = true
  }

  if (localStorageFlags.hasOwnProperty(flagName)) {
    return localStorageFlags[flagName] === _equals
  }

  if (CLOUD) {
    if (CLOUD_FLAGS.hasOwnProperty(flagName)) {
      return CLOUD_FLAGS[flagName] === _equals
    }

    return false
  }

  if (OSS_FLAGS.hasOwnProperty(flagName)) {
    return OSS_FLAGS[flagName] === _equals
  }

  return false
}

// type influx.toggleFeature('myFlag') to disable / enable any feature flag
export const FeatureFlag: FunctionComponent<{
  name: string
  equals?: string | boolean
}> = ({name, equals, children}) => {
  if (!isFlagEnabled(name, equals)) {
    return null
  }

  return children as any
}

export const getUserFlags = function getUserFlags() {
  const flagKeys = CLOUD ? Object.keys(CLOUD_FLAGS) : Object.keys(OSS_FLAGS)

  const flags = {}
  flagKeys.forEach(key => {
    flags[key] = isFlagEnabled(key)
  })

  return flags
}

/* eslint-disable no-console */
const list = () => {
  console.log('Currently Available Feature Flags')
  console.table(getUserFlags())
}
/* eslint-enable no-console */

const reset = () => {
  const featureFlags = JSON.parse(window.localStorage.featureFlags || '{}')

  if (CLOUD) {
    Object.keys(featureFlags).forEach(k => {
      if (!CLOUD_FLAGS.hasOwnProperty(k)) {
        delete featureFlags[k]
      } else {
        featureFlags[k] = CLOUD_FLAGS[k]
      }
    })
  } else {
    Object.keys(featureFlags).forEach(k => {
      if (!OSS_FLAGS.hasOwnProperty(k)) {
        delete featureFlags[k]
      } else {
        featureFlags[k] = OSS_FLAGS[k]
      }
    })
  }

  window.localStorage.featureFlags = JSON.stringify(featureFlags)
}

export const set = (flagName: string, value: string | boolean) => {
  const featureFlags = JSON.parse(window.localStorage.featureFlags || '{}')

  featureFlags[flagName] = value

  window.localStorage.featureFlags = JSON.stringify(featureFlags)

  return featureFlags[flagName]
}

export const toggleLocalStorageFlag = (flagName: string) => {
  const featureFlags = JSON.parse(window.localStorage.featureFlags || '{}')

  featureFlags[flagName] = !featureFlags[flagName]

  window.localStorage.featureFlags = JSON.stringify(featureFlags)

  return featureFlags[flagName]
}

// Expose utility in dev tools console for convenience
const w: any = window

w.influx = {toggleFeature: toggleLocalStorageFlag, list, reset, set}
