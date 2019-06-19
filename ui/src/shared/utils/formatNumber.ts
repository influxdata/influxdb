// Libraries
import {format} from 'd3-format'

// Types
import {Base} from 'src/types'

const MAX_DECIMALS = 4 // We should eventually make this configurable

const formatRaw = format(`.${MAX_DECIMALS}~f`) // e.g. "0.032"

const formatSIPrefix = format(`.${MAX_DECIMALS}~s`) // e.g. "2.452M"

const BINARY_PREFIXES = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']

const formatBinaryPrefix = (t: number): string => {
  const i = Math.floor(Math.log(Math.abs(t)) / Math.log(2 ** 10))

  return `${formatRaw(t / 1024 ** i)}${BINARY_PREFIXES[i]}`
}

export const formatNumber = (t: number, base: Base = ''): string => {
  const isSmallNumber = t >= -1 && t <= 1

  if (base === '2' && !isSmallNumber) {
    return formatBinaryPrefix(t)
  }

  if (base === '10' && !isSmallNumber) {
    return formatSIPrefix(t)
  }

  return formatRaw(t)
}
