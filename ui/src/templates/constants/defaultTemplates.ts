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
  'apache-data': apache,
  docker: docker,
  'getting-started-with-flux': gettingStarted,
  'github-data': github,
  jmeter: jmeter,
  kubernetes: kubernetes,
  nginx: nginx,
  'influxdb-2.0-oss-metrics': ossMetrics,
  redis: redis,
  system: system,
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
