import _ from 'lodash'

import AJAX from 'src/utils/ajax'

export const getSetupStatus = async (url: string): Promise<boolean> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url,
    })
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
  url: string,
  setupParams: SetupParams
): Promise<void> => {
  try {
    await AJAX({
      method: 'POST',
      url,
      data: setupParams,
    })
  } catch (error) {
    console.error("Can't set setup parameters", error)
    throw error
  }
}

export const signin = async (
  url: string,
  params: {username: string; password: string}
): Promise<void> => {
  const {username, password} = params
  try {
    await AJAX({
      method: 'POST',
      url,
      auth: {
        username,
        password,
      },
    })
  } catch (error) {
    console.error('Sign in has failed', error)
    throw error
  }
}

export const trySources = async (url: string): Promise<boolean> => {
  try {
    await AJAX({
      method: 'GET',
      url,
    })
    return true
  } catch (error) {
    console.error('Sign in has failed', error)
    return false
  }
}
