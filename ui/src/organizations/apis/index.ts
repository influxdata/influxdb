// Libraries
import AJAX from 'src/utils/ajax'
import _ from 'lodash'

// Types
import {Member, Bucket, Dashboard, Task, Organization} from 'src/types/v2'

// CRUD APIs for Organizations and Organization resources
// i.e. Organization Members, Buckets, Dashboards etc

export const getOrganizations = async (
  url: string
): Promise<Organization[]> => {
  const {data} = await AJAX({
    url,
  })

  return _.get(data, 'orgs', [])
}

export const createOrg = async (
  url: string,
  org: Partial<Organization>
): Promise<Organization> => {
  try {
    const {data} = await AJAX({
      url,
      method: 'POST',
      data: org,
    })

    return data
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

export const deleteOrg = async (url: string): Promise<void> => {
  try {
    await AJAX({
      url,
      method: 'DELETE',
    })
  } catch (error) {
    console.error('Could not delete org', error)
    throw error
  }
}

export const updateOrg = async (org: Organization): Promise<Organization> => {
  try {
    const {data} = await AJAX({
      url: org.links.self,
      method: 'PATCH',
      data: org,
    })

    return data
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

// Members
export const getMembers = async (url: string): Promise<Member[]> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return _.get(data, 'members', [])
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

// Buckets
export const getBuckets = async (url: string): Promise<Bucket[]> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data.buckets
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const createBucket = async (
  url: string,
  bucket: Partial<Bucket>
): Promise<Bucket> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: bucket,
    })

    return data
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const updateBucket = async (bucket: Bucket): Promise<Bucket> => {
  try {
    const {data} = await AJAX({
      url: bucket.links.self,
      method: 'PATCH',
      data: bucket,
    })

    return data
  } catch (error) {
    console.error('Could not get members for org', error)
    throw error
  }
}

export const getDashboards = async (url: string): Promise<Dashboard[]> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data.dashboards
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}

export const getTasks = async (url: string): Promise<Task[]> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return _.get(data, 'tasks', [])
  } catch (error) {
    console.error('Could not get tasks for org', error)
    throw error
  }
}
