export interface TaskOptions {
  name: string
  intervalTime: string
  intervalUnit: string
  delayTime: string
  delayUnit: string
  cron: string
  taskScheduleType: TaskSchedule
  orgID: string
}

export enum TaskSchedule {
  interval = 'INTERVAL',
  cron = 'CRON',
}

export const taskOptionsToFluxScript = (options: TaskOptions): string => {
  let fluxScript = `option task = { \n  name: "${options.name}",\n`

  if (options.taskScheduleType === TaskSchedule.interval) {
    fluxScript = `${fluxScript}  every: ${options.intervalTime}${
      options.intervalUnit
    },\n`
  } else if (options.taskScheduleType === TaskSchedule.cron) {
    fluxScript = `${fluxScript}  cron: "${options.cron}",\n`
  }
  fluxScript = `${fluxScript}  delay: ${options.delayTime}${options.delayUnit},`

  fluxScript = `${fluxScript}\n}`
  return fluxScript
}
