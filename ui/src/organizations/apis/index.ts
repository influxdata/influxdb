// Libraries
import _ from 'lodash'

import {client} from 'src/utils/api'

// Types
import {Organization} from '@influxdata/influx'
import {Dashboard} from 'src/types'

// CRUD APIs for Organizations and Organization resources
// i.e. Organization Members, Buckets, Dashboards etc

export const getDashboards = async (
  org?: Organization
): Promise<Dashboard[]> => {
  try {
    let result
    if (org) {
      result = await client.dashboards.getAll(org.id)
    } else {
      result = await client.dashboards.getAll()
    }

    return result
  } catch (error) {
    console.error('Could not get buckets for org', error)
    throw error
  }
}
