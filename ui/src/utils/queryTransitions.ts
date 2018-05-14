import _ from 'lodash'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  hasField,
  removeField,
  getFieldsDeep,
  getFuncsByFieldName,
} from 'src/shared/reducers/helpers/fields'

import {
  Tag,
  Field,
  GroupBy,
  Namespace,
  TagValues,
  TimeShift,
  QueryConfig,
  ApplyFuncsToFieldArgs,
} from 'src/types'

export const editRawText = (
  query: QueryConfig,
  rawText: string
): QueryConfig => {
  return {...query, rawText}
}

export const chooseNamespace = (
  query: QueryConfig,
  namespace: Namespace,
  isKapacitorRule: boolean = false
): QueryConfig => ({
  ...defaultQueryConfig({id: query.id, isKapacitorRule}),
  ...namespace,
})

export const chooseMeasurement = (
  query: QueryConfig,
  measurement: string,
  isKapacitorRule: boolean = false
): QueryConfig => ({
  ...defaultQueryConfig({id: query.id, isKapacitorRule}),
  database: query.database,
  retentionPolicy: query.retentionPolicy,
  measurement,
})

export const toggleKapaField = (query: QueryConfig, field: Field) => {
  if (field.type === 'field') {
    return {
      ...query,
      fields: [field],
      groupBy: {
        ...query.groupBy,
        time: null,
      },
    }
  }

  return {
    ...query,
    fields: [field],
  }
}

export const buildInitialField = (value: string): Field => ({
  type: 'func',
  alias: `mean_${value}`,
  args: [{value, type: 'field'}],
  value: 'mean',
})

export const addInitialField = (
  query: QueryConfig,
  field,
  groupBy
): QueryConfig => {
  return {
    ...query,
    fields: [buildInitialField(field.value)],
    groupBy,
  }
}

export const toggleField = (
  query: QueryConfig,
  {value}: Field
): QueryConfig => {
  const {fields = [], groupBy} = query
  const isSelected = hasField(value, fields)
  const newFuncs = fields.filter(f => f.type === 'func')

  if (isSelected) {
    // if list is all fields, remove that field
    // if list is all funcs, remove all funcs that match
    const newFields = removeField(value, fields)
    if (!newFields.length) {
      return {
        ...query,
        groupBy: {
          ...groupBy,
          time: null,
        },
        fields: [],
      }
    }
    return {
      ...query,
      fields: newFields,
    }
  }

  // if we are not applying functions apply a field
  if (!newFuncs.length) {
    return {
      ...query,
      fields: [...fields, {value, type: 'field'}],
    }
  }

  const defaultField = {
    type: 'func',
    alias: `mean_${value}`,
    args: [{value, type: 'field'}],
    value: 'mean',
  }

  return {
    ...query,
    fields: [...fields, defaultField],
  }
}

export const groupByTime = (query: QueryConfig, time: string): QueryConfig => ({
  ...query,
  groupBy: {...query.groupBy, time},
})

export const fill = (query: QueryConfig, value: string): QueryConfig => ({
  ...query,
  fill: value,
})

export const toggleTagAcceptance = (query: QueryConfig): QueryConfig => ({
  ...query,
  areTagsAccepted: !query.areTagsAccepted,
})

export const removeFuncs = (
  query: QueryConfig,
  fields: Field[]
): QueryConfig => ({
  ...query,
  fields: getFieldsDeep(fields),
  groupBy: {...query.groupBy, time: null},
})

export const applyFuncsToField = (
  query: QueryConfig,
  {field, funcs = []}: ApplyFuncsToFieldArgs,
  groupBy: GroupBy
): QueryConfig => {
  const nextFields = query.fields.reduce((acc, f) => {
    // If there is a func applied to only one field, add it to the other fields
    if (f.type === 'field') {
      return [
        ...acc,
        funcs.map(func => {
          const {value, type} = func
          const args = [{value: f.value, type: 'field'}]
          const alias = func.alias ? func.alias : `${func.value}_${f.value}`

          return {
            value,
            type,
            args,
            alias,
          }
        }),
      ]
    }

    const fieldToChange = f.args.find(a => a.value === field.value)
    // Apply new funcs to field
    if (fieldToChange) {
      const newFuncs = funcs.reduce((acc2, func) => {
        const funcsToChange = getFuncsByFieldName(fieldToChange.value, acc)
        const dup = funcsToChange.find(a => a.value === func.value)

        if (dup) {
          return acc2
        }

        return [
          ...acc2,
          {
            ...func,
            args: [field],
            alias: `${func.value}_${field.value}`,
          },
        ]
      }, [])

      return [...acc, ...newFuncs]
    }

    return [...acc, f]
  }, [])

  return {
    ...query,
    fields: _.flatten(nextFields),
    groupBy,
  }
}

export const updateRawQuery = (query: QueryConfig, rawText): QueryConfig => ({
  ...query,
  rawText,
})

export const groupByTag = (query: QueryConfig, tagKey: string): QueryConfig => {
  const oldTags = query.groupBy.tags
  let newTags

  // Toggle the presence of the tagKey
  if (oldTags.includes(tagKey)) {
    const i = oldTags.indexOf(tagKey)
    newTags = oldTags.slice()
    newTags.splice(i, 1)
  } else {
    newTags = oldTags.concat(tagKey)
  }

  return {
    ...query,
    groupBy: {...query.groupBy, tags: newTags},
  }
}

export const chooseTag = (query: QueryConfig, tag: Tag): QueryConfig => {
  const tagValues = query.tags[tag.key]
  const shouldRemoveTag =
    tagValues && tagValues.length === 1 && tagValues[0] === tag.value
  if (shouldRemoveTag) {
    const newTags = {...query.tags}
    delete newTags[tag.key]
    return {...query, tags: newTags}
  }

  const updateTagValues = (newTagValues: TagValues): QueryConfig => {
    return {
      ...query,
      tags: {
        ...query.tags,
        [tag.key]: newTagValues,
      },
    }
  }

  const oldTagValues = query.tags[tag.key]
  if (!oldTagValues) {
    return updateTagValues([tag.value])
  }

  // If the tag value is already selected, deselect it by removing it from the list
  const tagValuesCopy = oldTagValues.slice()
  const i = tagValuesCopy.indexOf(tag.value)
  if (i > -1) {
    tagValuesCopy.splice(i, 1)
    return updateTagValues(tagValuesCopy)
  }

  return updateTagValues(query.tags[tag.key].concat(tag.value))
}

export const timeShift = (query: QueryConfig, shift: TimeShift) => ({
  ...query,
  shifts: [shift],
})
