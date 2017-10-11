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

// functions returns all top-level fields with type
export const ofType = (fields, type) =>
  _.filter(fields, f => _.get(f, 'type') === type)

// functions returns all top-level functions in fields
export const functions = fields => ofType(fields, 'func')

// numFunctions searches queryConfig fields for functions
export const numFunctions = fields => _.size(functions(fields))

// functionNames returns the names of all top-level functions
export const functionNames = fields => functions(fields).map(f => f.name)

// getFields returns all of the top-level fields of type field
export const getFields = fields => ofType(fields, 'field')

export const getFieldsDeep = fields =>
  _.uniqBy(
    fieldWalk(fields, f => (_.get(f, 'type') === 'field' ? f : null)),
    'name'
  )

export const fieldNamesDeep = fields =>
  getFieldsDeep(fields).map(f => _.get(f, 'name'))

// firstFieldName returns the name of the first of type field
export const firstFieldName = fields => _.head(fieldNamesDeep(fields))

export const hasField = (fieldName, fields) =>
  fieldNamesDeep(fields).some(f => f === fieldName)

// everyOfType returns true if all top-level field types are type
export const everyOfType = (fields, type) =>
  fields.every(f => _.get(f, 'type') === type)

// everyField returns if all top-level field types are field
export const everyField = fields => everyOfType(fields, 'field')

// everyFunction returns if all top-level field types are functions
export const everyFunction = fields => everyOfType(fields, 'func')

// removeField will remove the field or function from the field list with the
// given fieldName. Preconditions: only type field OR only type func
export const removeField = (fieldName, fields) => {
  if (everyField(fields)) {
    return fields.filter(f => f.name !== fieldName)
  }

  return fields.reduce((acc, f) => {
    const has = fieldNamesDeep(f.args).some(n => n === fieldName)
    if (has) {
      return acc
    }

    return [...acc, f]
  }, [])
}
