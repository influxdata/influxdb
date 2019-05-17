import {
  system,
  localMetrics,
  gettingStarted,
} from '@influxdata/influxdb-templates'

export const localMetricsTemplate = () => {
  return localMetrics
}

export const systemTemplate = () => {
  return system
}

export const gettingStartedWithFluxTemplate = () => {
  return gettingStarted
}
