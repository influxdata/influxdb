var KMB_LABELS = ['K', 'M', 'B', 'T', 'Q']
var KMG2_BIG_LABELS = ['k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
var KMG2_SMALL_LABELS = ['m', 'u', 'n', 'p', 'f', 'a', 'z', 'y']

const pow = (base, exp) => {
  if (exp < 0) {
    return 1.0 / Math.pow(base, -exp)
  }

  return Math.pow(base, exp)
}

const round_ = (num, places) => {
  var shift = Math.pow(10, places)
  return Math.round(num * shift) / shift
}

export const numberValueFormatter = (x, opts) => {
  const sigFigs = opts('sigFigs')

  if (sigFigs !== null) {
    // User has opted for a fixed number of significant figures.
    return floatFormat(x, sigFigs)
  }

  const digits = opts('digitsAfterDecimal')
  const maxNumberWidth = opts('maxNumberWidth')

  const kmb = opts('labelsKMB')
  const kmg2 = opts('labelsKMG2')

  let label

  // switch to scientific notation if we underflow or overflow fixed display.
  if (
    x !== 0.0 &&
    (Math.abs(x) >= Math.pow(10, maxNumberWidth) ||
      Math.abs(x) < Math.pow(10, -digits))
  ) {
    label = x.toExponential(digits)
  } else {
    label = '' + round_(x, digits)
  }

  if (kmb || kmg2) {
    let k
    let k_labels = []
    let m_labels = []
    if (kmb) {
      k = 1000
      k_labels = KMB_LABELS
    }
    if (kmg2) {
      if (kmb) console.warn('Setting both labelsKMB and labelsKMG2. Pick one!')
      k = 1024
      k_labels = KMG2_BIG_LABELS
      m_labels = KMG2_SMALL_LABELS
    }

    let absx = Math.abs(x)
    let n = pow(k, k_labels.length)
    for (var j = k_labels.length - 1; j >= 0; j--, n /= k) {
      if (absx >= n) {
        label = round_(x / n, digits) + k_labels[j]
        break
      }
    }
    if (kmg2) {
      const x_parts = String(x.toExponential()).split('e-')
      if (x_parts.length === 2 && x_parts[1] >= 3 && x_parts[1] <= 24) {
        if (x_parts[1] % 3 > 0) {
          label = round_(x_parts[0] / pow(10, x_parts[1] % 3), digits)
        } else {
          label = Number(x_parts[0]).toFixed(2)
        }
        label += m_labels[Math.floor(x_parts[1] / 3) - 1]
      }
    }
  }

  return label
}

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
