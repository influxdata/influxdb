import {get} from 'lodash'
import {RATE_LIMIT_ERROR_STATUS} from 'src/shared/constants/errors'
import {LimitsState} from 'src/cloud/reducers/limits'

export const isLimitError = error => {
  return get(error, 'response.status', '') === RATE_LIMIT_ERROR_STATUS
}

export const extractMessage = error => {
  return get(error, 'response.data.message', '')
}

export const extractBucketLimits = (limits: LimitsState) => {
  return get(limits, 'buckets.limitStatus')
}

export const extractBucketMax = (limits: LimitsState) => {
  return get(limits, 'buckets.maxAllowed', Infinity)
}

export const extractDashboardLimits = (limits: LimitsState) => {
  return get(limits, 'dashboards.limitStatus')
}

export const extractDashboardMax = (limits: LimitsState) => {
  return get(limits, 'dashboard.maxAllowed', Infinity)
}

export const extractTaskLimits = (limits: LimitsState) => {
  return get(limits, 'tasks.limitStatus')
}

export const extractTaskMax = (limits: LimitsState) => {
  return get(limits, 'task.maxAllowed', Infinity)
}
