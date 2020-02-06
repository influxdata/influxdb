// Types
import {AppState, Check, ThresholdCheck, DeadmanCheck} from 'src/types'
import {PostCheck} from 'src/client'

// Utils
import {checkThresholdsValid} from './checkValidate'
import {isDurationParseable} from 'src/shared/utils/duration'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getOrg} from 'src/organizations/selectors'

type AlertBuilder = AppState['alertBuilder']

export const toPostCheck = (check: Check): PostCheck => {
  // TODO: type PostCheck properly github.com/influxdata/influxdb/issues/16704
  const status = check.activeStatus

  delete check.activeStatus

  return {
    ...check,
    status,
  } as PostCheck
}

export const builderToPostCheck = (state: AppState) => {
  const {alertBuilder} = state
  const check = genCheckBase(state)

  validateBuilder(alertBuilder)

  if (check.type === 'threshold') {
    return toThresholdPostCheck(alertBuilder, check)
  }

  if (check.type === 'deadman') {
    return toDeadManPostCheck(alertBuilder, check)
  }
}

const toDeadManPostCheck = (
  alertBuilder: AlertBuilder,
  check: DeadmanCheck
): PostCheck => {
  const {
    every,
    level,
    offset,
    reportZero,
    staleTime,
    statusMessageTemplate,
    tags,
    timeSince,
    activeStatus,
  } = alertBuilder

  if (!isDurationParseable(timeSince) || !isDurationParseable(staleTime)) {
    throw new Error('Duration fields must contain valid duration')
  }

  return {
    ...check,
    every,
    level,
    offset,
    reportZero,
    staleTime,
    statusMessageTemplate,
    tags,
    timeSince,
    status: activeStatus,
  }
}

const toThresholdPostCheck = (
  alertBuilder: AlertBuilder,
  check: ThresholdCheck
): PostCheck => {
  const {
    activeStatus,
    every,
    offset,
    statusMessageTemplate,
    tags,
    thresholds,
  } = alertBuilder

  checkThresholdsValid(thresholds)

  return {
    ...check,
    every,
    offset,
    statusMessageTemplate,
    tags,
    thresholds,
    status: activeStatus,
  }
}

const validateBuilder = (alertBuilder: AlertBuilder) => {
  if (!isDurationParseable(alertBuilder.offset)) {
    throw new Error('Check offset must be a valid duration')
  }

  if (!isDurationParseable(alertBuilder.every)) {
    throw new Error('Check every must be a valid duration')
  }
}

const genCheckBase = (state: AppState): Check => {
  const {type, id, status, activeStatus, name} = state.alertBuilder
  const {draftQueries} = getActiveTimeMachine(state)
  const {id: orgID} = getOrg(state)

  return {
    id,
    type,
    status,
    activeStatus,
    name,
    query: draftQueries[0],
    orgID,
    labels: [],
  } as Check
}
