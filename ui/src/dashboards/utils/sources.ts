export const nextSource = (prevQuery, nextQuery) => {
  if (nextQuery.source) {
    return nextQuery.source
  }

  return prevQuery.source
}
