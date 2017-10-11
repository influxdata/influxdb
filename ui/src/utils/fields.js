import _ from 'lodash'

// fieldWalk traverses fields rescursively into args
export const fieldWalk = (fields, fn) =>
  fields.each(f => _.concat(fn(f), fieldWalk(_.get(f, 'args', []), fn)))

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

export const fieldsDeep = fields =>
  _.uniqBy(fieldWalk(fields, f => getFields(f)), 'name')

export const fieldNamesDeep = fields =>
  fieldsDeep(fields).map(f => _.get(f, 'name'))

// firstFieldName returns the name of the first of type field
export const firstFieldName = fields => _.head(fieldNamesDeep(fields))
