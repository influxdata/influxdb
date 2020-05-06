import {format} from 'd3-format'

const MAX_DECIMALS = 4

const formatSmallNumber = format(`.${MAX_DECIMALS}~f`) // e.g. "0.032"

const formatLargeNumber = format(`.${MAX_DECIMALS}~s`) // e.g. "2.452M"

export const defaultNumberFormatter = (t: number) => {
  if (t >= -1 && t <= 1) {
    return formatSmallNumber(t)
  }

  return formatLargeNumber(t)
}
