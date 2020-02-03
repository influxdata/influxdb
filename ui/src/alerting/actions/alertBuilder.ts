// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

// Types
import {
  RemoteDataState,
  CheckTagSet,
  Threshold,
  CheckStatusLevel,
  CheckType,
  Check,
} from 'src/types'

export type Action =
  | ReturnType<typeof resetAlertBuilder>
  | ReturnType<typeof initializeAlertBuilder>
  | ReturnType<typeof convertCheckToCustom>
  | ReturnType<typeof setAlertBuilderCheck>
  | ReturnType<typeof setAlertBuilderCheckStatus>
  | ReturnType<typeof changeCheckType>
  | ReturnType<typeof setEvery>
  | ReturnType<typeof setOffset>
  | ReturnType<typeof setStaleTime>
  | ReturnType<typeof setTimeSince>
  | ReturnType<typeof setLevel>
  | ReturnType<typeof setStatusMessageTemplate>
  | ReturnType<typeof editTagSetByIndex>
  | ReturnType<typeof removeTagSet>
  | ReturnType<typeof updateThreshold>
  | ReturnType<typeof updateThresholds>
  | ReturnType<typeof removeThreshold>
  | ReturnType<typeof updateName>

export const resetAlertBuilder = () => ({
  type: 'RESET_ALERT_BUILDER' as 'RESET_ALERT_BUILDER',
})

export const initializeAlertBuilder = (type: CheckType) => ({
  type: 'INIT_ALERT_BUILDER' as 'INIT_ALERT_BUILDER',
  payload: {type},
})

export const convertCheckToCustom = () => ({
  type: 'CONVERT_CHECK_TO_CUSTOM' as 'CONVERT_CHECK_TO_CUSTOM',
})

export const setAlertBuilderCheck = (check: Check) => ({
  type: 'SET_ALERT_BUILDER_CHECK' as 'SET_ALERT_BUILDER_CHECK',
  payload: {check},
})

export const setAlertBuilderCheckStatus = (status: RemoteDataState) => ({
  type: 'SET_ALERT_BUILDER_STATUS' as 'SET_ALERT_BUILDER_STATUS',
  payload: {status},
})

export const changeCheckType = (toType: CheckType) => ({
  type: 'SET_ALERT_BUILDER_TYPE' as 'SET_ALERT_BUILDER_TYPE',
  payload: {toType},
})

export const setEvery = (every: string) => ({
  type: 'SET_ALERT_BUILDER_EVERY' as 'SET_ALERT_BUILDER_EVERY',
  payload: {every},
})

export const setOffset = (offset: string) => ({
  type: 'SET_ALERT_BUILDER_OFFSET' as 'SET_ALERT_BUILDER_OFFSET',
  payload: {offset},
})

export const setStaleTime = (staleTime: string) => ({
  type: 'SET_ALERT_BUILDER_STALETIME' as 'SET_ALERT_BUILDER_STALETIME',
  payload: {staleTime},
})

export const setTimeSince = (timeSince: string) => ({
  type: 'SET_ALERT_BUILDER_TIMESINCE' as 'SET_ALERT_BUILDER_TIMESINCE',
  payload: {timeSince},
})

export const setLevel = (level: CheckStatusLevel) => ({
  type: 'SET_ALERT_BUILDER_LEVEL' as 'SET_ALERT_BUILDER_LEVEL',
  payload: {level},
})

export const setStatusMessageTemplate = (statusMessageTemplate: string) => ({
  type: 'SET_ALERT_BUILDER_MESSAGE_TEMPLATE' as 'SET_ALERT_BUILDER_MESSAGE_TEMPLATE',
  payload: {statusMessageTemplate},
})

export const editTagSetByIndex = (
  index: number,
  tagSet: Required<CheckTagSet>
) => ({
  type: 'EDIT_ALERT_BUILDER_TAGSET' as 'EDIT_ALERT_BUILDER_TAGSET',
  payload: {index, tagSet},
})

export const removeTagSet = (index: number) => ({
  type: 'REMOVE_ALERT_BUILDER_TAGSET' as 'REMOVE_ALERT_BUILDER_TAGSET',
  payload: {index},
})

export const updateThreshold = (threshold: Threshold) => ({
  type: 'UPDATE_ALERT_BUILDER_THRESHOLD' as 'UPDATE_ALERT_BUILDER_THRESHOLD',
  payload: {threshold},
})

export const updateThresholds = (thresholds: Threshold[]) => ({
  type: 'UPDATE_ALERT_BUILDER_THRESHOLDS' as 'UPDATE_ALERT_BUILDER_THRESHOLDS',
  payload: {thresholds},
})

export const removeThreshold = (level: CheckStatusLevel) => ({
  type: 'REMOVE_ALERT_BUILDER_THRESHOLD' as 'REMOVE_ALERT_BUILDER_THRESHOLD',
  payload: {level},
})

export const updateName = (name: string) => ({
  type: 'UPDATE_ALERT_BUILDER_NAME' as 'UPDATE_ALERT_BUILDER_NAME',
  payload: {name},
})

export const selectCheckEvery = (every: string) => dispatch => {
  dispatch(setEvery(every))
  dispatch(executeQueries())
}
