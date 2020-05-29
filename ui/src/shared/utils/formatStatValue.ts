// Libraries
import {isNumber, isString} from 'lodash'

// Types
import {DecimalPlaces} from 'src/types/dashboards'

// Constants
import {MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

// Utils
import {preventNegativeZero} from 'src/shared/utils/preventNegativeZero'

interface FormatStatValueOptions {
  decimalPlaces?: DecimalPlaces
  prefix?: string
  suffix?: string
}

export const formatStatValue = (
  value: number | string = 0,
  {decimalPlaces, prefix, suffix}: FormatStatValueOptions = {}
): string => {
  let localeFormattedValue: undefined | string | number

  if (isNumber(value)) {
    let digits: number

    if (decimalPlaces && decimalPlaces.isEnforced) {
      digits = decimalPlaces.digits
    } else {
      digits = getAutoDigits(value)
    }

    const roundedValue = value.toFixed(digits)

    localeFormattedValue =
      Number(roundedValue) === 0
        ? roundedValue
        : Number(roundedValue).toLocaleString(undefined, {
            maximumFractionDigits: MAX_DECIMAL_PLACES,
          })
  } else if (isString(value)) {
    localeFormattedValue = value
  } else {
    return 'Data cannot be displayed'
  }

  localeFormattedValue = preventNegativeZero(localeFormattedValue)
  const formattedValue = `${prefix || ''}${localeFormattedValue}${suffix || ''}`

  return formattedValue
}

const getAutoDigits = (value: number): number => {
  const decimalIndex = value.toString().indexOf('.')

  return decimalIndex === -1 ? 0 : 2
}
