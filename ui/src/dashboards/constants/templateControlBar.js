import _ from 'lodash'
import calculateSize from 'calculate-size'

export const minDropdownWidth = 146
export const maxDropdownWidth = 300
export const dropdownPadding = 30

const valueLength = a => _.size(a.value)

export const calculateDropdownWidth = (values = []) => {
  const longestValue = _.maxBy(values, valueLength)
  const longestValuePixels =
    calculateSize(longestValue, {
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
