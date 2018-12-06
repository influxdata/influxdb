export interface TaskOptions {
  name: string
  interval: string
  cron: string
  offset: string
  taskScheduleType: TaskSchedule
  orgID: string
}

export type TaskOptionKeys = keyof TaskOptions

export enum TaskSchedule {
  interval = 'interval',
  cron = 'cron',
  unselected = '',
}

export const taskOptionsToFluxScript = (options: TaskOptions): string => {
  let fluxScript = `option task = { \n  name: "${options.name}",\n`

  if (options.taskScheduleType === TaskSchedule.interval) {
    fluxScript = `${fluxScript}  every: ${options.interval},\n`
  } else if (options.taskScheduleType === TaskSchedule.cron) {
    fluxScript = `${fluxScript}  cron: "${options.cron}",\n`
  }

  if (options.offset) {
    fluxScript = `${fluxScript}  offset: ${options.offset}\n`
  }

  fluxScript = `${fluxScript}}`
  return fluxScript
}
