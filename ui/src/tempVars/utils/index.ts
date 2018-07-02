export const trimAndRemoveQuotes = elt => {
  const trimmed = elt.trim()
  const dequoted = trimmed.replace(/(^")|("$)/g, '')

  return dequoted
}

export const formatTempVar = name =>
  `:${name.replace(/:/g, '').replace(/\s/g, '')}:`
