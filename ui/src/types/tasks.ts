import {Task as ITask} from 'src/client'
import {NormalizedState, Run, RemoteDataState, LogEvent} from 'src/types'

export interface Task extends Omit<ITask, 'labels'> {
  labels?: string[]
}
export interface TaskOptions {
  name: string
  interval: string
  cron: string
  offset: string
  taskScheduleType: TaskSchedule
  orgID: string
  toOrgName: string
  toBucketName: string
}

export interface TasksState extends NormalizedState<Task> {
  newScript: string
  currentScript: string
  currentTask: Task
  searchTerm: string
  showInactive: boolean
  taskOptions: TaskOptions
  runs: Run[]
  runStatus: RemoteDataState
  logs: LogEvent[]
}

export enum TaskSchedule {
  interval = 'interval',
  cron = 'cron',
  unselected = '',
}

export type TaskOptionKeys = keyof TaskOptions
