import _ from 'lodash'

// fieldWalk traverses fields rescursively into args mapping fn on every
// field
export const fieldWalk = (fields, fn, acc = []) =>
  _.compact(
    _.flattenDeep(
      fields.reduce((a, f) => {
        return [...a, fn(f), fieldWalk(_.get(f, 'args', []), fn, acc)]
      }, acc)
    )
  )

// ofType returns all top-level fields with type
export const ofType = (fields, type) =>
  _.filter(fields, f => _.get(f, 'type') === type)

// getFields returns all of the top-level fields of type field
export const getFields = fields => ofType(fields, 'field')

// getFunctions returns all top-level functions in fields
export const getFunctions = fields => ofType(fields, 'func')

// numFunctions searches queryConfig fields for functions and counts them
export const numFunctions = fields => _.size(getFunctions(fields))

// functionNames returns the value of all top-level functions
export const functionNames = fields => getFunctions(fields).map(f => f.value)

// returns a flattened list of all fieldNames in a queryConfig
export const getFieldsDeep = fields =>
  _.uniqBy(
    fieldWalk(fields, f => (_.get(f, 'type') === 'field' ? f : null)),
    'value'
  )

export const fieldNamesDeep = fields =>
  getFieldsDeep(fields).map(f => _.get(f, 'value'))

// firstFieldName returns the value of the first of type field
export const firstFieldName = fields => _.head(fieldNamesDeep(fields))

export const hasField = (fieldName, fields) =>
  fieldNamesDeep(fields).some(f => f === fieldName)

// getAllFields and funcs with fieldName
export const getFieldsWithName = (fieldName, fields) =>
  getFieldsDeep(fields).filter(f => f.value === fieldName)

// everyOfType returns true if all top-level field types are type
export const everyOfType = (fields, type) =>
  fields.every(f => _.get(f, 'type') === type)

// everyField returns if all top-level field types are field
export const everyField = fields => everyOfType(fields, 'field')

// everyFunction returns if all top-level field types are functions
export const everyFunction = fields => everyOfType(fields, 'func')

export const getFuncsByFieldName = (fieldName, fields) =>
  getFunctions(fields).filter(f =>
    _.get(f, 'args', []).some(a => a.value === fieldName)
  )

// removeField will remove the field or function from the field list with the
// given fieldName. Preconditions: only type field OR only type func
export const removeField = (fieldName, fields) => {
  if (everyField(fields)) {
    return fields.filter(f => f.value !== fieldName)
  }

  return fields.reduce((acc, f) => {
    const has = fieldNamesDeep(f.args).some(n => n === fieldName)
    if (has) {
      return acc
    }

    return [...acc, f]
  }, [])
}
