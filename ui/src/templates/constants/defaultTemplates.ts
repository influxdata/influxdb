import SYSTEM_TEMPLATE from 'src/templates/constants/system.json'
import GETTING_STARTED from 'src/templates/constants/gettingStarted.json'

import LOCAL_METRICS from 'src/templates/constants/localMetrics.json'

export const localMetricsTemplate = () => {
  return LOCAL_METRICS
}

export const systemTemplate = () => {
  return SYSTEM_TEMPLATE
}

export const gettingStartedWithFluxTemplate = () => {
  return GETTING_STARTED
}
