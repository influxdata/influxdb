import _ from 'lodash'

export interface Duration {
  days: number
  hours: number
  minutes: number
  seconds: number
}

const secondsToDuration = (seconds: number): Duration => {
  let minutes = Math.floor(seconds / 60)
  seconds = seconds % 60
  let hours = Math.floor(minutes / 60)
  minutes = minutes % 60
  const days = Math.floor(hours / 24)
  hours = hours % 24

  return {
    days,
    hours,
    minutes,
    seconds,
  }
}

export const ruleToString = (seconds: number): string => {
  const duration = secondsToDuration(seconds)
  const rpString = Object.entries(duration).reduce((acc, [k, v]) => {
    if (!v) {
      return acc
    }

    return `${acc} ${v} ${k}`
  }, '')

  if (!rpString) {
    return 'forever'
  }

  return rpString
}
