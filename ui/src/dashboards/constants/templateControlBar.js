import calculateSize from 'calculate-size'

export const minDropdownWidth = 146
export const maxDropdownWidth = 300
export const dropdownPadding = 30

export const calculateDropdownWidth = values => {
  const longestValue = values.reduce(
    (a, b) => (a.value.length > b.value.length ? a.value : b.value)
  )
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
