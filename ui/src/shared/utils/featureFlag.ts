import {FunctionComponent} from 'react'
import {CLOUD} from 'src/shared/constants'

const OSS_FLAGS = {
  heatmap: true,
  scatter: true,
  lineGraphShading: true,
}

const CLOUD_FLAGS = {
  heatmap: false, // We need to ensure the API updates have been deployed before enabling
  scatter: false, // ditto ^^
  lineGraphShading: false, // ditto! ^^
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
