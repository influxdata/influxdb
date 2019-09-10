import {get} from 'lodash'
import {ASSET_LIMIT_ERROR_STATUS} from 'src/cloud/constants/index'
import {LimitsState} from 'src/cloud/reducers/limits'
import {LimitStatus} from 'src/cloud/actions/limits'

export const isLimitError = (error): boolean => {
  return get(error, 'response.status', '') === ASSET_LIMIT_ERROR_STATUS
}

export const extractBucketLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'buckets.limitStatus')
}
export const extractBucketMax = (limits: LimitsState): number => {
  return get(limits, 'buckets.maxAllowed') || Infinity // if maxAllowed == 0, there are no limits on asset
}

export const extractBucketMaxRetentionSeconds = (
  limits: LimitsState
): number => {
  return get(limits, 'buckets.maxRetentionSeconds', null)
}

export const extractDashboardLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'dashboards.limitStatus')
}
export const extractDashboardMax = (limits: LimitsState): number => {
  return get(limits, 'dashboards.maxAllowed') || Infinity
}

export const extractTaskLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'tasks.limitStatus')
}
export const extractTaskMax = (limits: LimitsState): number => {
  return get(limits, 'tasks.maxAllowed') || Infinity
}

export const extractChecksMax = (limits: LimitsState): number => {
  return get(limits, 'checks.maxAllowed') || Infinity
}

export const extractRulesMax = (limits: LimitsState): number => {
  return get(limits, 'rules.maxAllowed') || Infinity
}
export const extractBlockedRules = (limits: LimitsState): string[] => {
  return get(limits, 'rules.blocked') || []
}

export const extractEndpointsMax = (limits: LimitsState): number => {
  return get(limits, 'endpoints.maxAllowed') || Infinity
}
export const extractBlockedEndpoints = (limits: LimitsState): string[] => {
  return get(limits, 'endpoints.blocked') || []
}

export const extractMonitoringLimitStatus = (
  limits: LimitsState
): LimitStatus => {
  const statuses = [
    get(limits, 'rules.limitStatus'),
    get(limits, 'endpoints.limitStatus'),
    get(limits, 'checks.limitStatus'),
  ]

  if (statuses.includes(LimitStatus.EXCEEDED)) {
    return LimitStatus.EXCEEDED
  }

  return LimitStatus.OK
}

export const extractLimitedMonitoringResources = (
  limits: LimitsState
): string => {
  const rateLimitedResources = []

  if (get(limits, 'checks.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('checks')
  }

  if (get(limits, 'rules.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('rules')
  }

  if (get(limits, 'endpoints.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('endpoints')
  }

  return rateLimitedResources.join(', ')
}

export const extractRateLimitStatus = (limits: LimitsState): LimitStatus => {
  const statuses = [
    get(limits, 'rate.writeKBs.limitStatus'),
    get(limits, 'rate.readKBs.limitStatus'),
    get(limits, 'rate.cardinality.limitStatus'),
  ]

  if (statuses.includes(LimitStatus.EXCEEDED)) {
    return LimitStatus.EXCEEDED
  }

  return LimitStatus.OK
}

export const extractRateLimitResources = (limits: LimitsState): string[] => {
  const rateLimitedResources = []

  if (get(limits, 'rate.readKBs.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('read')
  }

  if (get(limits, 'rate.writeKBs.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('write')
  }

  if (get(limits, 'rate.cardinality.limitStatus') === LimitStatus.EXCEEDED) {
    rateLimitedResources.push('cardinality')
  }

  return rateLimitedResources
}
