import {Task as TaskAPI, Organization} from '@influxdata/influx'

export interface Task extends TaskAPI {
  organization: Organization
}
