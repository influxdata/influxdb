export const resolveSelectedValue = (
  values: string[],
  prevSelection?: string,
  defaultSelection?: string
): string => {
  if (values.includes(prevSelection)) {
    return prevSelection
  }

  if (values.includes(defaultSelection)) {
    return defaultSelection
  }

  return values[0]
}
