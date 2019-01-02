import {orgsAPI, bucketsAPI, dashboardsAPI, taskAPI} from 'src/utils/api'

// Types
import {Bucket, Dashboard, Task, Organization, User, Label} from 'src/api'

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
export const getMembers = async (org: Organization): Promise<User[]> => {
  try {
    const {data} = await orgsAPI.orgsOrgIDMembersGet(org.id)

    return data.users
  } catch (error) {
    console.error('Could not get members for org', error)
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
    console.error('Could not get members for org', error)
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

export const getLabels = async (__: Organization): Promise<Label[]> => {
  // Use try catch when accessing the actual API
  // TODO: Delete this silly mocks
  const mockLabels: Label[] = [
    {
      name: 'Swogglez',
      properties: {
        description: 'I am an example Label',
        color: '#ff0054',
      },
    },
    {
      name: 'Top Secret',
      properties: {
        description: 'Only admins can modify these resources',
        color: '#4a52f4',
      },
    },
    {
      name: 'Pineapples',
      properties: {
        description: 'Pineapples are in my head',
        color: '#f4c24a',
      },
    },
    {
      name: 'SWAT',
      properties: {
        description: 'Boots and cats and boots and cats',
        color: '#d6ff9c',
      },
    },
    {
      name: 'the GOAT',
      properties: {
        description: 'Gatsby obviously ate turnips',
        color: '#17d9f0',
      },
    },
    {
      name: 'My Spoon is Too Big',
      properties: {
        description: 'My Spooooooooon is Too Big',
        color: '#27c27e',
      },
    },
  ]

  return mockLabels
}

// TODO: implement with an actual API call
export const createLabel = async (
  __: Organization,
  label: Label
): Promise<Label> => {
  return label
}

// TODO: implement with an actual API call
export const deleteLabel = async (
  __: Organization,
  ___: Label
): Promise<void> => {}

// TODO: implement with an actual API call
export const updateLabel = async (
  __: Organization,
  label: Label
): Promise<Label> => {
  return label
}
