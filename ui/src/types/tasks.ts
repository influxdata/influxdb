import {Task as TaskAPI, ITask} from '@influxdata/influx'

export const TaskStatus = TaskAPI.StatusEnum
export interface Task extends ITask {}
