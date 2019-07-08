// Libraries
import uuid from 'uuid'

// Constants
import {
  THRESHOLD_COLORS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  COLOR_TYPE_THRESHOLD,
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
  BASE_THRESHOLD_ID,
} from 'src/shared/constants/thresholds'

// Types
import {Color} from 'src/types'

/*
  Sort a list of thresholds for rendering.

  - Base or minimum thresholds come first
  - Max thresholds come last
  - All other thresholds are sorted by value

*/
export const sortThresholds = (thresholds: Color[]): Color[] => {
  const result = [...thresholds]

  result.sort((a, b) =>
    a.id === BASE_THRESHOLD_ID ? -Infinity : a.value - b.value
  )

  return result
}

/*
  Given a list of thresholds, return an object of error messages for any
  invalid threshold in the list.

  A threshold is invalid if:

  - Its value is NaN
  - Its value is less than the min threshold in the list
  - Its value is more than the max threshold in the list

*/
export const validateThresholds = (
  thresholds: Color[]
): {[thresholdID: string]: string} => {
  const minThreshold = thresholds.find(({type}) => type === COLOR_TYPE_MIN)
  const maxThreshold = thresholds.find(({type}) => type === COLOR_TYPE_MAX)
  const errors = {}

  for (const {id, value, type} of thresholds) {
    if (isNaN(value)) {
      errors[id] = 'Please enter a valid number'
    } else if (
      minThreshold &&
      type !== COLOR_TYPE_MIN &&
      value < minThreshold.value
    ) {
      errors[id] = 'Please enter a value greater than the minimum threshold'
    } else if (
      maxThreshold &&
      type !== COLOR_TYPE_MAX &&
      value > maxThreshold.value
    ) {
      errors[id] = 'Please enter a value less than the maximum threshold'
    }
  }

  return errors
}

/*
  Given a list of thresholds, produce a new threshold that is suitable for
  adding to the list.
*/
export const addThreshold = (thresholds: Color[]): Color => {
  const values = thresholds.map(threshold => threshold.value)

  let minValue = Math.min(...values)
  let maxValue = Math.max(...values)

  if (minValue === Infinity || isNaN(minValue) || minValue === maxValue) {
    minValue = DEFAULT_VALUE_MIN
    maxValue = DEFAULT_VALUE_MAX
  }

  const value = randomTick(minValue, maxValue)

  const colorChoice =
    THRESHOLD_COLORS[Math.floor(Math.random() * THRESHOLD_COLORS.length)]

  const firstThresholdType = thresholds[0].type

  const thresholdType =
    firstThresholdType === COLOR_TYPE_MIN ||
    firstThresholdType === COLOR_TYPE_MAX
      ? COLOR_TYPE_THRESHOLD
      : firstThresholdType

  const threshold = {
    ...colorChoice,
    id: uuid.v4(),
    type: thresholdType,
    value,
  }

  return threshold
}

/*
  Generate a nice random number between `min` and `max` inclusive.
*/
const randomTick = (min: number, max: number): number => {
  const domainWidth = max - min

  let roundTo

  if (domainWidth > 1000) {
    roundTo = 100
  } else if (domainWidth > 100) {
    roundTo = 10
  } else if (domainWidth > 50) {
    roundTo = 5
  } else if (domainWidth > 10) {
    roundTo = 1
  } else {
    roundTo = null
  }

  let value: number

  if (roundTo) {
    value = Math.round((Math.random() * (max - min)) / roundTo) * roundTo
  } else {
    value = Number((Math.random() * (max - min)).toFixed(2))
  }

  return value
}
