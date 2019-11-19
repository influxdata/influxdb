import {FunctionComponent} from 'react'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

const OSS_FLAGS = {
  alerting: false,
  eventMarkers: false,
  deleteWithPredicate: false,
  monacoEditor: false,
  downloadCellCSV: false,
}

const CLOUD_FLAGS = {
  alerting: true,
  eventMarkers: false,
  deleteWithPredicate: false,
  monacoEditor: false,
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
  downloadCellCSV: false,
}

export const isFlagEnabled = (flagName: string) => {
  let localStorageFlags

  try {
    localStorageFlags = JSON.parse(window.localStorage.featureFlags)
  } catch {
    localStorageFlags = {}
  }

  return (
    localStorageFlags[flagName] === true ||
    (CLOUD && CLOUD_FLAGS[flagName]) ||
    (!CLOUD && OSS_FLAGS[flagName])
  )
}

// type influx.toggleFeature('myFlag') to disable / enable any feature flag
export const FeatureFlag: FunctionComponent<{name: string}> = ({
  name,
  children,
}) => {
  if (!isFlagEnabled(name)) {
    return null
  }

  return children as any
}

export const NegativeFeatureFlag: FunctionComponent<{name: string}> = ({
  name,
  children,
}) => {
  if (isFlagEnabled(name)) {
    return null
  }

  return children as any
}

/* eslint-disable no-console */
const list = () => {
  console.log('Currently Available Feature Flags')
  if (CLOUD) {
    console.table(CLOUD_FLAGS)
  } else {
    console.table(OSS_FLAGS)
  }
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
      if (!CLOUD_FLAGS.hasOwnProperty(k)) {
        delete featureFlags[k]
      } else {
        featureFlags[k] = CLOUD_FLAGS[k]
      }
    })
  }

  window.localStorage.featureFlags = JSON.stringify(featureFlags)
}

export const toggleLocalStorageFlag = (flagName: string) => {
  const featureFlags = JSON.parse(window.localStorage.featureFlags || '{}')

  featureFlags[flagName] = !featureFlags[flagName]

  window.localStorage.featureFlags = JSON.stringify(featureFlags)

  return featureFlags[flagName]
}

// Expose utility in dev tools console for convenience
const w: any = window

w.influx = {toggleFeature: toggleLocalStorageFlag, list, reset}
