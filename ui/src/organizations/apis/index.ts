// Libraries
import _ from 'lodash'

// Utils
import {getDeep} from 'src/utils/wrappers'

import {
  orgsAPI,
  bucketsAPI,
  dashboardsAPI,
  taskAPI,
  telegrafsAPI,
} from 'src/utils/api'

// Types
import {Bucket, Task, Organization, ResourceOwner, Telegraf} from 'src/api'
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

// Buckets
export const getBuckets = async (org: Organization): Promise<Bucket[]> => {
  try {
    const {data} = await bucketsAPI.bucketsGet(org.name)

    return data.buckets
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const createBucket = async (
  org: Organization,
  bucket: Bucket
): Promise<Bucket> => {
  try {
    const {data} = await bucketsAPI.bucketsPost(org.name, bucket)

    return data
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const updateBucket = async (bucket: Bucket): Promise<Bucket> => {
  try {
    const {data} = await bucketsAPI.bucketsBucketIDPatch(bucket.id, bucket)

    return data
  } catch (error) {
    console.error('Could not update bucket for org', error)
    throw error
  }
}

export const deleteBucket = async (bucket: Bucket): Promise<void> => {
  try {
    await bucketsAPI.bucketsBucketIDDelete(bucket.id)
  } catch (error) {
    console.error('Could not delete buckets from org', error)
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

export const getTasks = async (org: Organization): Promise<Task[]> => {
  try {
    const {data} = await taskAPI.tasksGet(null, null, org.name)

    return data.tasks
  } catch (error) {
    console.error('Could not get tasks for org', error)
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
