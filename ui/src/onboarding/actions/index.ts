// Constants
import {StepStatus} from 'src/clockface/constants/wizard'
import {SetupSuccess, SetupError} from 'src/shared/copy/notifications'

// Actions
import {notify} from 'src/shared/actions/notifications'

import {client} from 'src/utils/api'

// Types
import {SetupParams} from 'src/onboarding/apis'

export type Action =
  | SetSetupParams
  | SetStepStatus
  | SetOrganizationID
  | SetBucketID

interface SetSetupParams {
  type: 'SET_SETUP_PARAMS'
  payload: {setupParams: SetupParams}
}

export const setSetupParams = (setupParams: SetupParams): SetSetupParams => ({
  type: 'SET_SETUP_PARAMS',
  payload: {setupParams},
})

interface SetStepStatus {
  type: 'SET_STEP_STATUS'
  payload: {index: number; status: StepStatus}
}

export const setStepStatus = (
  index: number,
  status: StepStatus
): SetStepStatus => ({
  type: 'SET_STEP_STATUS',
  payload: {
    index,
    status,
  },
})

interface SetOrganizationID {
  type: 'SET_ORG_ID'
  payload: {orgID: string}
}

const setOrganizationID = (orgID: string): SetOrganizationID => ({
  type: 'SET_ORG_ID',
  payload: {orgID},
})

interface SetBucketID {
  type: 'SET_BUCKET_ID'
  payload: {bucketID: string}
}

export const setBucketID = (bucketID: string): SetBucketID => ({
  type: 'SET_BUCKET_ID',
  payload: {bucketID},
})

export const setupAdmin = (params: SetupParams) => async dispatch => {
  try {
    dispatch(setSetupParams(params))
    const response = await client.setup.create(params)

    const {id: orgID} = response.org
    const {id: bucketID} = response.bucket

    dispatch(setOrganizationID(orgID))
    dispatch(setBucketID(bucketID))

    const {username, password} = params

    await client.auth.signin(username, password)
    dispatch(notify(SetupSuccess))
  } catch (err) {
    console.error(err)
    dispatch(notify(SetupError))
  }
}
