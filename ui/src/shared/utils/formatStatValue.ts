import {MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

import {DecimalPlaces} from 'src/types/v2/dashboards'

interface FormatStatValueOptions {
  decimalPlaces?: DecimalPlaces
  prefix?: string
  suffix?: string
}

export const formatStatValue = (
  value: number = 0,
  {decimalPlaces, prefix, suffix}: FormatStatValueOptions = {}
): string => {
  let digits: number

  if (decimalPlaces && decimalPlaces.isEnforced) {
    digits = decimalPlaces.digits
  } else {
    digits = getAutoDigits(value)
  }

  const roundedValue = value.toFixed(digits)

  const localeFormattedValue = Number(roundedValue).toLocaleString(undefined, {
    maximumFractionDigits: MAX_DECIMAL_PLACES,
  })

  const formattedValue = `${prefix || ''}${localeFormattedValue}${suffix || ''}`

  return formattedValue
}

const getAutoDigits = (value: number): number => {
  const decimalIndex = value.toString().indexOf('.')

  return decimalIndex === -1 ? 0 : 2
}
