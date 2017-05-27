import _ from 'lodash'

export const deepDiffByKeys = (sourceObj, comparatorObj) => {
  const keysWithDiffValues = []

  // perform a deep diff on props objects to see what keys have changed
  // from https://stackoverflow.com/questions/8572826/generic-deep-diff-between-two-objects
  _.mergeWith(sourceObj, comparatorObj, (objectValue, sourceValue, key) => {
    if (
      !_.isEqual(objectValue, sourceValue) &&
      Object(objectValue) !== objectValue
    ) {
      keysWithDiffValues.push(key)
    }
  })

  return keysWithDiffValues
}
