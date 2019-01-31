// Libraries
import _ from 'lodash'

// Utils
import {getDeep} from 'src/utils/wrappers'

import {dashboardsAPI, telegrafsAPI} from 'src/utils/api'

// Types
import {Organization, Telegraf} from 'src/api'
import {Dashboard} from 'src/types/v2'

// CRUD APIs for Organizations and Organization resources
// i.e. Organization Members, Buckets, Dashboards etc

export const getDashboards = async (
  org?: Organization
): Promise<Dashboard[]> => {
  try {
    let result
    if (org) {
      const {data} = await dashboardsAPI.dashboardsGet(org.name)
      result = data.dashboards
    } else {
      const {data} = await dashboardsAPI.dashboardsGet(null)
      result = data.dashboards
    }

    return result
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const getCollectors = async (org: Organization): Promise<Telegraf[]> => {
  try {
    const data = await telegrafsAPI.telegrafsGet(org.id)

    return getDeep<Telegraf[]>(data, 'data.configurations', [])
  } catch (error) {
    console.error(error)
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

export const deleteTelegrafConfig = async (
  telegrafID: string
): Promise<Telegraf> => {
  try {
    const response = await telegrafsAPI.telegrafsTelegrafIDDelete(telegrafID)

    return response.data
  } catch (error) {
    console.error(error)
  }
}
