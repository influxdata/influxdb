import _ from 'lodash'
import calculateSize from 'calculate-size'

export const minDropdownWidth = 120
export const maxDropdownWidth = 330
export const dropdownPadding = 30

const valueLength = a => _.size(a.value)

export const calculateDropdownWidth = values => {
  if (!values || !values.length) {
    return minDropdownWidth
  }

  const longest = _.maxBy(values, valueLength)

  const longestValuePixels =
    calculateSize(longest.value, {
      font: 'Monospace',
      fontSize: '12px',
    }).width + dropdownPadding

  if (longestValuePixels < minDropdownWidth) {
    return minDropdownWidth
  }

  if (longestValuePixels > maxDropdownWidth) {
    return maxDropdownWidth
  }

  return longestValuePixels
}
