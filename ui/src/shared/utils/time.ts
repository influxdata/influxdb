import moment from 'moment'

const dateFormat = 'YYYY-MM-DD HH:mm'

export const formatTimeRange = (timeRange: string | null): string => {
  if (!timeRange) {
    return ''
  }

  if (timeRange === 'now()') {
    return moment(new Date()).format(dateFormat)
  }

  if (timeRange.match(/^now/)) {
    const [, duration, unitOfTime] = timeRange.match(/(\d+)(\w+)/)
    const d = duration as moment.unitOfTime.DurationConstructor
    return moment(new Date())
      .subtract(d, unitOfTime)
      .format(dateFormat)
  }

  return moment(timeRange.replace(/\'/g, '')).format(dateFormat)
}
