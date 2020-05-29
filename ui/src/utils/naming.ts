export const incrementCloneName = (
  namesList: string[],
  cloneName: string
): string => {
  const root = cloneName.replace(/\s\(clone\s(\d)+\)/g, '').replace(/\)/, '')

  const filteredNames = namesList.filter(n => n.includes(root))

  const highestNumberedClone = filteredNames.reduce((acc, name) => {
    if (name.match(/\(clone(\s|\d)+\)/)) {
      const strippedName = name
        .replace(root, '')
        .replace(/\(clone/, '')
        .replace(/\)/, '')

      const cloneNumber = Number(strippedName)

      return cloneNumber >= acc ? cloneNumber : acc
    }

    return acc
  }, 0)

  if (highestNumberedClone) {
    const newCloneNumber = highestNumberedClone + 1
    return `${cloneName.replace(
      /\(clone\s(\d)+\)/,
      ''
    )} (clone ${newCloneNumber})`
  }

  return `${cloneName} (clone 1)`
}
