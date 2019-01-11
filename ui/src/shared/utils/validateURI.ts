export const validateURI = (value: string): boolean => {
  const regex = /http[s]?:\/\//

  if (regex.test(value)) {
    return true
  }
  return false
}
