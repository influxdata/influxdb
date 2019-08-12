import {
  apache,
  docker,
  gettingStarted,
  github,
  kubernetes,
  nginx,
  ossMetrics,
  redis,
  system,
} from '@influxdata/influxdb-templates'

export const staticTemplates = {
  Apache: apache,
  Docker: docker,
  'getting-started': gettingStarted,
  Github: github,
  Kubernetes: kubernetes,
  Nginx: nginx,
  'oss-metrics': ossMetrics,
  Redis: redis,
  System: system,
}

export const influxdbTemplateList = [
  apache,
  docker,
  gettingStarted,
  github,
  kubernetes,
  nginx,
  ossMetrics,
  redis,
  system,
].map((t, i) => ({...t, id: `influxdb-template-${i}`}))
