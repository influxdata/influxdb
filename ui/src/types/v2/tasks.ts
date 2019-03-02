import {Task as TaskAPI, Organization, User, Label} from '@influxdata/influx'

export interface Task extends Exclude<TaskAPI, 'labels'> {
  labels: Label[]
  organization?: Organization
  owner?: User
}

export const TaskStatus = TaskAPI.StatusEnum
