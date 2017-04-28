export const formatBytes = bytes => {
  if (bytes === 0) {
    return '0 Bytes'
  }

  if (!bytes) {
    return null
  }

  const k = 1000
  const dm = 2
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}

export const formatRPDuration = duration => {
  if (!duration) {
    return
  }

  if (duration === '0' || duration === '0s') {
    return '∞'
  }

  let adjustedTime = duration
  const [_, hours, minutes, seconds] = duration.match(/(\d*)h(\d*)m(\d*)s/) // eslint-disable-line no-unused-vars
  const hoursInDay = 24
  if (hours > hoursInDay) {
    const remainder = hours % hoursInDay
    const days = (hours - remainder) / hoursInDay
    adjustedTime = `${days}d`
    adjustedTime += +remainder === 0 ? '' : `${remainder}h`
    adjustedTime += +minutes === 0 ? '' : `${minutes}m`
    adjustedTime += +seconds === 0 ? '' : `${seconds}s`
  } else {
    adjustedTime = `${hours}h`
    adjustedTime += +minutes === 0 ? '' : `${minutes}m`
    adjustedTime += +seconds === 0 ? '' : `${seconds}s`
  }

  return adjustedTime
}
