import {Task as TaskAPI} from '@influxdata/influx'
import {Task as ITask} from 'src/client'
import {Label} from 'src/types'

export const TaskStatus = TaskAPI.StatusEnum
export interface Task extends ITask {
  lastRunError?: string
  lastRunStatus?: 'failed' | 'success' | 'canceled'
  labels?: Label[]
}
