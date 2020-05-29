export const pluralize = (collection: any): string =>
  Object.keys(collection).length === 1 ? '' : 's'
