// Libraries
import _ from 'lodash'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'
import {SetupSuccess, SetupError} from 'src/shared/copy/notifications'

// Actions
import {notify} from 'src/shared/actions/notifications'

// APIs
import {client} from 'src/utils/api'
import * as api from 'src/client'

// Types
import {AppThunk} from 'src/types'
import {ISetupParams} from '@influxdata/influx'

export type Action =
  | SetSetupParams
  | SetStepStatus
  | SetOrganizationID
  | SetBucketID

interface SetSetupParams {
  type: 'SET_SETUP_PARAMS'
  payload: {setupParams: ISetupParams}
}

export const setSetupParams = (setupParams: ISetupParams): SetSetupParams => ({
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
  type: 'SET_ONBOARDING_BUCKET_ID'
  payload: {bucketID: string}
}

export const setBucketID = (bucketID: string): SetBucketID => ({
  type: 'SET_ONBOARDING_BUCKET_ID',
  payload: {bucketID},
})

export const setupAdmin = (
  params: ISetupParams
): AppThunk<Promise<boolean>> => async dispatch => {
  try {
    dispatch(setSetupParams(params))
    const response = await client.setup.create(params)

    const {id: orgID} = response.org
    const {id: bucketID} = response.bucket

    dispatch(setOrganizationID(orgID))
    dispatch(setBucketID(bucketID))

    const {username, password} = params

    const resp = await api.postSignin({auth: {username, password}})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(notify(SetupSuccess))
    dispatch(setStepStatus(1, StepStatus.Complete))
    return true
  } catch (err) {
    console.error(err)
    const message = _.get(err, 'response.data.message', '')
    dispatch(notify(SetupError(message)))
    dispatch(setStepStatus(1, StepStatus.Error))
  }
}
