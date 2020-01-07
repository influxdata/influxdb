export const preventNegativeZero = (
  value: number | string
): number | string => {
  if (Number(value) === -0) {
    return typeof value === 'number' ? 0 : value.replace(/-/g, '')
  }
  return value
}
