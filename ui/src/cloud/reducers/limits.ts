import {produce} from 'immer'

//Types
import {Actions, ActionTypes} from 'src/cloud/actions/limits'
import {RemoteDataState} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

interface Limit {
  maxAllowed: number
  limitStatus: LimitStatus
}

interface LimitWithBlocked extends Limit {
  blocked: string[]
}

export interface LimitsState {
  dashboards: Limit
  tasks: Limit
  buckets: Limit & {maxRetentionSeconds: number | null}
  checks: Limit
  rules: LimitWithBlocked
  endpoints: LimitWithBlocked
  rate: {
    readKBs: Limit
    writeKBs: Limit
    cardinality: Limit
  }
  status: RemoteDataState
}

const defaultLimit: Limit = {
  maxAllowed: Infinity,
  limitStatus: LimitStatus.OK,
}

const defaultLimitWithBlocked: LimitWithBlocked = {...defaultLimit, blocked: []}

export const defaultState: LimitsState = {
  dashboards: defaultLimit,
  tasks: defaultLimit,
  buckets: {...defaultLimit, maxRetentionSeconds: null},
  checks: defaultLimit,
  rules: defaultLimitWithBlocked,
  endpoints: defaultLimitWithBlocked,
  rate: {
    readKBs: defaultLimit,
    writeKBs: defaultLimit,
    cardinality: defaultLimit,
  },
  status: RemoteDataState.NotStarted,
}

export const limitsReducer = (
  state = defaultState,
  action: Actions
): LimitsState =>
  produce(state, draftState => {
    switch (action.type) {
      case ActionTypes.SetLimitsStatus: {
        const {status} = action.payload
        draftState.status = status
        return
      }

      case ActionTypes.SetLimits: {
        const {limits} = action.payload

        if (limits.bucket) {
          const {maxBuckets, maxRetentionDuration} = limits.bucket
          draftState.buckets.maxAllowed = maxBuckets
          draftState.buckets.maxRetentionSeconds = maxRetentionDuration / 1e9
        }

        if (limits.dashboard) {
          const {maxDashboards} = limits.dashboard
          draftState.dashboards.maxAllowed = maxDashboards
        }

        if (limits.task) {
          const {maxTasks} = limits.task
          draftState.tasks.maxAllowed = maxTasks
        }

        if (limits.check) {
          const {maxChecks} = limits.check
          draftState.checks.maxAllowed = maxChecks
        }

        if (limits.notificationRule) {
          const {
            maxNotifications,
            blockedNotificationRules,
          } = limits.notificationRule
          draftState.rules.maxAllowed = maxNotifications
          draftState.rules.blocked = blockedNotificationRules
            .split(',')
            .map(r => r.trim())
        }

        if (limits.notificationEndpoint) {
          const {blockedNotificationEndpoints} = limits.notificationEndpoint
          draftState.endpoints.blocked = blockedNotificationEndpoints
            .split(',')
            .map(r => r.trim())
        }

        if (limits.rate) {
          const {readKBs, writeKBs, cardinality} = limits.rate

          draftState.rate.readKBs.maxAllowed = readKBs
          draftState.rate.writeKBs.maxAllowed = writeKBs
          draftState.rate.cardinality.maxAllowed = cardinality
        }

        return
      }

      case ActionTypes.SetDashboardLimitStatus: {
        draftState.dashboards.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetBucketLimitStatus: {
        draftState.buckets.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetTaskLimitStatus: {
        draftState.tasks.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetChecksLimitStatus: {
        draftState.checks.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetRulesLimitStatus: {
        draftState.rules.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetEndpointsLimitStatus: {
        draftState.endpoints.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetReadRateLimitStatus: {
        draftState.rate.readKBs.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetWriteRateLimitStatus: {
        draftState.rate.writeKBs.limitStatus = action.payload.limitStatus
        return
      }

      case ActionTypes.SetCardinalityLimitStatus: {
        draftState.rate.cardinality.limitStatus = action.payload.limitStatus
        return
      }
    }
  })

export default limitsReducer
