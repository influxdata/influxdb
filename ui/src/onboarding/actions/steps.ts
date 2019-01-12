// Constants
import {StepStatus} from 'src/clockface/constants/wizard'
import {SetupSuccess, SetupError} from 'src/shared/copy/notifications'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Types
import {
  SetupParams,
  signin as signinAJAX,
  setSetupParams as setSetupParamsAJAX,
} from 'src/onboarding/apis'

export type Action = SetSetupParams | SetStepStatus | SetOrganizationID

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
  payload: {organizationID}
}

const setOrganizationID = organizationID => ({
  type: 'SET_ORG_ID',
  payload: {organizationID},
})

export const setupAdmin = (setupParams: SetupParams) => async dispatch => {
  try {
    dispatch(setSetupParams(setupParams))
    const onboardingResponse = await setSetupParamsAJAX(setupParams)
    const {id: organizationID} = onboardingResponse.org

    dispatch(setOrganizationID(organizationID))

    await signinAJAX({
      username: setupParams.username,
      password: setupParams.password,
    })
    dispatch(notify(SetupSuccess))
  } catch (err) {
    console.error(err)
    dispatch(notify(SetupError))
  }
}
