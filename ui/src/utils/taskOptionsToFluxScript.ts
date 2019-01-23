import _ from 'lodash'

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

export const addDestinationToFluxScript = (
  script: string,
  options: TaskOptions
): string => {
  const {toOrgName, toBucketName} = options
  if (toOrgName && toBucketName) {
    const trimmedScript = _.trimEnd(script)
    const trimmedOrgName = toOrgName.trim()
    const trimmedBucketName = toBucketName.trim()
    return `${trimmedScript}\n  |> to(bucket: "${trimmedBucketName}", org: "${trimmedOrgName}")`
  }

  return script
}
