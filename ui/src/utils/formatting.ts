import _ from 'lodash'
const KMB_LABELS: string[] = ['K', 'M', 'B', 'T', 'Q']
const KMG2_BIG_LABELS: string[] = ['k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
const KMG2_SMALL_LABELS: string[] = ['m', 'u', 'n', 'p', 'f', 'a', 'z', 'y']

const pow = (base: number, exp: number): number => {
  if (exp < 0) {
    return 1.0 / Math.pow(base, -exp)
  }

  return Math.pow(base, exp)
}

const roundNum = (num, places): number => {
  const shift = Math.pow(10, places)
  return Math.round(num * shift) / shift
}

const floatFormat = (x: number, optPrecision: number): string => {
  // Avoid invalid precision values; [1, 21] is the valid range.
  const p = Math.min(Math.max(1, optPrecision || 2), 21)

  // This is deceptively simple.  The actual algorithm comes from:
  //
  // Max allowed length = p + 4
  // where 4 comes from 'e+n' and '.'.
  //
  // Length of fixed format = 2 + y + p
  // where 2 comes from '0.' and y = # of leading zeroes.
  //
  // Equating the two and solving for y yields y = 2, or 0.00xxxx which is
  // 1.0e-3.
  //
  // Since the behavior of toPrecision() is identical for larger numbers, we
  // don't have to worry about the other bound.
  //
  // Finally, the argument for toExponential() is the number of trailing digits,
  // so we take off 1 for the value before the '.'.
  return Math.abs(x) < 1.0e-3 && x !== 0.0
    ? x.toExponential(p - 1)
    : x.toPrecision(p)
}

// taken from https://github.com/danvk/dygraphs/blob/aaec6de56dba8ed712fd7b9d949de47b46a76ccd/src/dygraph-utils.js#L1103
export const numberValueFormatter = (
  x: number,
  opts: (name: string) => number,
  prefix: string,
  suffix: string
): string => {
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
    label = `${roundNum(x, digits)}`
  }

  if (kmb || kmg2) {
    let k
    let kLabels = []
    let mLabels = []
    if (kmb) {
      k = 1000
      kLabels = KMB_LABELS
    }
    if (kmg2) {
      if (kmb) {
        console.error('Setting both labelsKMB and labelsKMG2. Pick one!')
      }
      k = 1024
      kLabels = KMG2_BIG_LABELS
      mLabels = KMG2_SMALL_LABELS
    }

    const absx = Math.abs(x)
    let n = pow(k, kLabels.length)
    for (let j = kLabels.length - 1; j >= 0; j -= 1, n /= k) {
      if (absx >= n) {
        label = roundNum(x / n, digits) + kLabels[j]
        break
      }
    }
    if (kmg2) {
      const xParts = String(x.toExponential())
        .split('e-')
        .map(Number)
      if (xParts.length === 2 && xParts[1] >= 3 && xParts[1] <= 24) {
        if (xParts[1] % 3 > 0) {
          label = roundNum(xParts[0] / pow(10, xParts[1] % 3), digits)
        } else {
          label = Number(xParts[0]).toFixed(2)
        }
        label += mLabels[Math.floor(xParts[1] / 3) - 1]
      }
    }
  }

  return `${prefix}${label}${suffix}`
}

export const formatRPDuration = (duration: string | null): string => {
  if (!duration) {
    return
  }

  if (duration === '0' || duration === '0s') {
    return '∞'
  }

  let adjustedTime = duration
  const durationMatcher = /(?:(\d*)d)?(?:(\d*)h)?(?:(\d*)m)?(?:(\d*)s)?/
  const result = duration.match(durationMatcher)
  const days = _.get<string, string>(result, 1, '0')
  const hours = _.get<string, string>(result, 2, '0')
  const minutes = _.get<string, string>(result, 3, '0')
  const seconds = _.get<string, string>(result, 4, '0')

  const hoursInDay = 24
  if (+days) {
    adjustedTime = `${days}d`
    adjustedTime += +hours === 0 ? '' : `${hours}h`
    adjustedTime += +minutes === 0 ? '' : `${minutes}m`
    adjustedTime += +seconds === 0 ? '' : `${seconds}s`
  } else if (+hours > hoursInDay) {
    const hoursRemainder = +hours % hoursInDay
    const daysQuotient = (+hours - hoursRemainder) / hoursInDay
    adjustedTime = `${daysQuotient}d`
    adjustedTime += +hoursRemainder === 0 ? '' : `${hoursRemainder}h`
    adjustedTime += +minutes === 0 ? '' : `${minutes}m`
    adjustedTime += +seconds === 0 ? '' : `${seconds}s`
  } else {
    adjustedTime = `${hours}h`
    adjustedTime += +minutes === 0 ? '' : `${minutes}m`
    adjustedTime += +seconds === 0 ? '' : `${seconds}s`
  }

  return adjustedTime
}
