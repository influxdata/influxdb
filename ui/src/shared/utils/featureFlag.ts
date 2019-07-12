import {FunctionComponent} from 'react'
import {CLOUD, CLOUD_BILLING_VISIBLE} from 'src/shared/constants'

const OSS_FLAGS = {
  alerting: false,
}

const CLOUD_FLAGS = {
  alerting: false,
  cloudBilling: CLOUD_BILLING_VISIBLE, // should be visible in dev and acceptance, but not in cloud
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

export const FeatureFlag: FunctionComponent<{name: string}> = ({
  name,
  children,
}) => {
  if (!isFlagEnabled(name)) {
    return null
  }

  return children as any
}

export const toggleLocalStorageFlag = (flagName: string) => {
  const featureFlags = JSON.parse(window.localStorage.featureFlags || '{}')

  featureFlags[flagName] = !featureFlags[flagName]

  window.localStorage.featureFlags = JSON.stringify(featureFlags)

  return featureFlags[flagName]
}

// Expose utility in dev tools console for convenience
const w: any = window

w.influx = {toggleFeature: toggleLocalStorageFlag}
