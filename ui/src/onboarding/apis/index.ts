// Libraries
import _ from 'lodash'

import {setupAPI, sourcesAPI} from 'src/utils/api'

// Utils
import {
  telegrafsAPI,
  authorizationsAPI,
  writeAPI,
  scraperTargetsApi,
} from 'src/utils/api'

import {
  Telegraf,
  WritePrecision,
  TelegrafRequest,
  OnboardingResponse,
  ScraperTargetRequest,
  ScraperTargetResponse,
} from 'src/api'

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

export const getTelegrafConfig = async (
  telegrafConfigID
): Promise<Telegraf> => {
  try {
    const response = await telegrafsAPI.telegrafsTelegrafIDGet(telegrafConfigID)
    return response.data
  } catch (error) {
    console.error(error)
    return null
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
  telegrafConfig: TelegrafRequest
): Promise<Telegraf> => {
  try {
    const {data} = await telegrafsAPI.telegrafsPost(telegrafConfig)

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

export const createScraperTarget = async (
  url: string,
  orgID: string,
  bucketID: string
): Promise<ScraperTargetResponse> => {
  const response = await scraperTargetsApi.scrapersPost({
    name: 'new target',
    type: ScraperTargetRequest.TypeEnum.Prometheus,
    url,
    orgID,
    bucketID,
  })

  return response.data
}

export const updateScraperTarget = async (
  id: string,
  url: string,
  bucketID: string
): Promise<ScraperTargetResponse> => {
  const response = await scraperTargetsApi.scrapersScraperTargetIDPatch(id, {
    url,
    bucketID,
  })

  return response.data
}
