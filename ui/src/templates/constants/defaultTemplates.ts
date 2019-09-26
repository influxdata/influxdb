import {
  apache,
  docker,
  gettingStarted,
  github,
  jmeter,
  kubernetes,
  nginx,
  ossMetrics,
  redis,
  system,
} from '@influxdata/influxdb-templates'
import {DashboardTemplate} from 'src/types'

export const ossMetricsTemplate = (): DashboardTemplate => {
  return ossMetrics
}

export const staticTemplates: {[k: string]: DashboardTemplate} = {
  Apache: apache,
  Docker: docker,
  'getting-started': gettingStarted,
  Github: github,
  JMeter: jmeter,
  Kubernetes: kubernetes,
  Nginx: nginx,
  'oss-metrics': ossMetrics,
  Redis: redis,
  System: system,
}

export const influxdbTemplateList: DashboardTemplate[] = [
  apache,
  docker,
  gettingStarted,
  github,
  jmeter,
  kubernetes,
  nginx,
  ossMetrics,
  redis,
  system,
].map((t, i) => ({...t, id: `influxdb-template-${i}`}))
