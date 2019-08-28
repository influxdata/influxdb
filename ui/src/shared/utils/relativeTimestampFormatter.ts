import moment from 'moment'

export const relativeTimestampFormatter = (
  time: string,
  prefix?: string
): string => {
  const timeFromNow = moment(time).fromNow()

  if (prefix) {
    return `${prefix}${timeFromNow}`
  }

  return timeFromNow
}
