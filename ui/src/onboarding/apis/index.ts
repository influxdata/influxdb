// Libraries
import _ from 'lodash'

import {baseAPI, setupAPI, sourcesAPI} from 'src/utils/api'

// Utils
import {telegrafsAPI, authorizationsAPI, writeAPI} from 'src/utils/api'
import {Telegraf, WritePrecision, TelegrafRequest} from 'src/api'

import {getDeep} from 'src/utils/wrappers'

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

export const getTelegrafConfigTOML = async (
  telegrafID: string
): Promise<string> => {
  const options = {
    headers: {
      Accept: 'application/toml',
    },
  }

  const response = await telegrafsAPI.telegrafsTelegrafIDGet(
    telegrafID,
    options
  )

  return response.data as string // response.data is string with 'application/toml' header
}

export interface SetupParams {
  username: string
  password: string
  org: string
  bucket: string
}

export const setSetupParams = async (
  setupParams: SetupParams
): Promise<void> => {
  try {
    await setupAPI.setupPost(setupParams)
  } catch (error) {
    console.error("Can't set setup parameters", error)
    throw error
  }
}

export const signin = async (params: {
  username: string
  password: string
}): Promise<void> => {
  const {username, password} = params
  try {
    await baseAPI.signinPost({auth: {username, password}})
  } catch (error) {
    console.error('Sign in has failed', error)
    throw error
  }
}

export const trySources = async (): Promise<boolean> => {
  try {
    await sourcesAPI.sourcesGet(null)
    return true
  } catch (error) {
    console.error('Sign in has failed', error)
    return false
  }
}

export const getTelegrafConfigs = async (org: string): Promise<Telegraf[]> => {
  try {
    const data = await telegrafsAPI.telegrafsGet(org)

    return getDeep<Telegraf[]>(data, 'data.configurations', [])
  } catch (error) {
    console.error(error)
  }
}

export const getAuthorizationToken = async (
  username: string
): Promise<string> => {
  try {
    const data = await authorizationsAPI.authorizationsGet(undefined, username)
    return getDeep<string>(data, 'data.authorizations.0.token', '')
  } catch (error) {
    console.error(error)
  }
}

export const writeLineProtocol = async (
  org: string,
  bucket: string,
  body: string,
  precision: WritePrecision
): Promise<any> => {
  const data = await writeAPI.writePost(
    org,
    bucket,
    body,
    undefined,
    undefined,
    undefined,
    undefined,
    precision
  )
  return data
}

export const createTelegrafConfig = async (
  org: string,
  telegrafConfig: TelegrafRequest
): Promise<Telegraf> => {
  try {
    const {data} = await telegrafsAPI.telegrafsPost(org, telegrafConfig)

    return data
  } catch (error) {
    console.error(error)
  }
}

export const updateTelegrafConfig = async (
  telegrafID: string,
  telegrafConfig: TelegrafRequest
): Promise<Telegraf> => {
  try {
    const {data} = await telegrafsAPI.telegrafsTelegrafIDPut(
      telegrafID,
      telegrafConfig
    )

    return data
  } catch (error) {
    console.error(error)
  }
}
