export const preventNegativeZero = (
  value: number | string
): number | string => {
  // eslint-disable-next-line no-compare-neg-zero
  if (Number(value) === -0) {
    return typeof value === 'number' ? 0 : value.replace(/-/g, '')
  }
  return value
}
