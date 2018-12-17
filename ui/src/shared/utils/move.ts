export const move = <T>(values: T[], fromIndex: number, toIndex: number) => {
  if (fromIndex === toIndex) {
    return values.slice()
  }

  const targetValue = values[fromIndex]

  const valuesRemoved = [
    ...values.slice(0, fromIndex),
    ...values.slice(fromIndex + 1),
  ]

  return [
    ...valuesRemoved.slice(0, toIndex),
    targetValue,
    ...valuesRemoved.slice(toIndex),
  ]
}
