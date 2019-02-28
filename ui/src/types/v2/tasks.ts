import {Task as TaskAPI, Organization, User} from '@influxdata/influx'
import {Label} from 'src/types/v2/labels'

export interface Task extends Exclude<TaskAPI, 'labels'> {
  labels: Label[]
  organization: Organization
  owner?: User
  offset?: string
}

export const TaskStatus = TaskAPI.StatusEnum
