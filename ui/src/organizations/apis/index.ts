// API
import {getDashboards as apiGetDashboards} from 'src/client'

// Types
import {Dashboard, Organization} from 'src/types'

// Utils
import {addDashboardDefaults} from 'src/dashboards/actions'

// CRUD APIs for Organizations and Organization resources
// i.e. Organization Members, Buckets, Dashboards etc

export const getDashboards = async (
  org?: Organization
): Promise<Dashboard[]> => {
  try {
    let result
    if (org) {
      result = await apiGetDashboards({query: {orgID: org.id}})
    } else {
      result = await apiGetDashboards({})
    }

    if (result.status !== 200) {
      throw new Error(result.data.message)
    }

    const dashboards = result.data.map(d => addDashboardDefaults(d))

    return dashboards
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}
