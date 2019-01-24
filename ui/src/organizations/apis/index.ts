// Libraries
import _ from 'lodash'

// Utils
import {getDeep} from 'src/utils/wrappers'

import {
  orgsAPI,
  dashboardsAPI,
  telegrafsAPI,
  scraperTargetsApi,
} from 'src/utils/api'

// Types
import {
  Organization,
  ResourceOwner,
  Telegraf,
  ScraperTargetResponses,
} from 'src/api'
import {Dashboard} from 'src/types/v2'

// CRUD APIs for Organizations and Organization resources
// i.e. Organization Members, Buckets, Dashboards etc

export const getOrganizations = async (): Promise<Organization[]> => {
  const {data} = await orgsAPI.orgsGet()

  return data.orgs
}

export const createOrg = async (org: Organization): Promise<Organization> => {
  try {
    const {data} = await orgsAPI.orgsPost(org)

    return data
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

export const deleteOrg = async (org: Organization): Promise<void> => {
  try {
    await orgsAPI.orgsOrgIDDelete(org.id)
  } catch (error) {
    console.error('Could not delete org', error)
    throw error
  }
}

export const updateOrg = async (org: Organization): Promise<Organization> => {
  try {
    const {data} = await orgsAPI.orgsOrgIDPatch(org.id, org)

    return data
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

// Members
export const getMembers = async (
  org: Organization
): Promise<ResourceOwner[]> => {
  try {
    const {data} = await orgsAPI.orgsOrgIDMembersGet(org.id)

    return data.users
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

export const getOwners = async (
  org: Organization
): Promise<ResourceOwner[]> => {
  try {
    const {data} = await orgsAPI.orgsOrgIDOwnersGet(org.id)

    return data.users
  } catch (error) {
    console.error('Could not get owners for org', error)
    throw error
  }
}

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

// Scrapers
export const getScrapers = async (): Promise<ScraperTargetResponses> => {
  try {
    const response = await scraperTargetsApi.scrapersGet()

    return response.data
  } catch (error) {
    console.error(error)
  }
}

export const deleteScraper = async (scraperTargetID: string): Promise<void> => {
  try {
    await scraperTargetsApi.scrapersScraperTargetIDDelete(scraperTargetID)
  } catch (error) {
    console.error(error)
  }
}
