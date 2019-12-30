import {Task as TaskAPI, ITask} from '@influxdata/influx'

export const TaskStatus = TaskAPI.StatusEnum
export interface Task extends ITask {
  lastRunError?: string
  lastRunStatus?: 'failed' | 'success' | 'canceled'
}
