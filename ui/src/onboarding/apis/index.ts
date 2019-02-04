// Libraries
import _ from 'lodash'

// Utils
import {setupAPI, sourcesAPI} from 'src/utils/api'

import {OnboardingResponse} from 'src/api'

export const getSetupStatus = async (): Promise<boolean> => {
  try {
    const {data} = await setupAPI.setupGet()
    const {allowed} = data
    return allowed
  } catch (error) {
    console.error("Can't get setup status", error)
    throw error
  }
}

export interface SetupParams {
  username: string
  password: string
  org: string
  bucket: string
}

export const setSetupParams = async (
  setupParams: SetupParams
): Promise<OnboardingResponse> => {
  try {
    const result = await setupAPI.setupPost(setupParams)
    return result.data
  } catch (error) {
    console.error("Can't set setup parameters", error)
    throw error
  }
}

export const trySources = async (): Promise<boolean> => {
  try {
    await sourcesAPI.sourcesGet('')
    return true
  } catch (error) {
    console.error('Sign in has failed', error)
    return false
  }
}
