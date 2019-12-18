import {MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

import {DecimalPlaces} from 'src/types/dashboards'

interface FormatStatValueOptions {
  decimalPlaces?: DecimalPlaces
  prefix?: string
  suffix?: string
}

export const formatStatValue = (
  value: number | string = 0,
  {decimalPlaces, prefix, suffix}: FormatStatValueOptions = {}
): string => {
  let localeFormattedValue = ''

  if (typeof value === 'number') {
    let digits: number

    if (decimalPlaces && decimalPlaces.isEnforced) {
      digits = decimalPlaces.digits
    } else {
      digits = getAutoDigits(value)
    }

    const roundedValue = value.toFixed(digits)

    localeFormattedValue = Number(roundedValue).toLocaleString(undefined, {
      maximumFractionDigits: MAX_DECIMAL_PLACES,
    })
  } else if (typeof value === 'string') {
    localeFormattedValue = value
  } else {
    return 'Data cannot be displayed'
  }

  const formattedValue = `${prefix || ''}${localeFormattedValue}${suffix || ''}`

  return formattedValue
}

const getAutoDigits = (value: number): number => {
  const decimalIndex = value.toString().indexOf('.')

  return decimalIndex === -1 ? 0 : 2
}
