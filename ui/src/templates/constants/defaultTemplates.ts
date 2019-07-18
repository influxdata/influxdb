import {
  system,
  localMetrics,
  gettingStarted,
  docker,
  nginx,
  redis,
  kubernetes,
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

export const staticTemplates = {
  'getting-started': gettingStarted,
  'local-metrics': localMetrics,
  System: system,
  Docker: docker,
  Redis: redis,
  Nginx: nginx,
  Kubernetes: kubernetes,
}

export const influxdbTemplateList = [
  system,
  localMetrics,
  gettingStarted,
  docker,
  nginx,
  redis,
  kubernetes,
].map((t, i) => ({...t, id: `influxdb-template-${i}`}))
