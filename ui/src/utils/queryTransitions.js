import _ from 'lodash'
import defaultQueryConfig from 'utils/defaultQueryConfig'
import {
  hasField,
  removeField,
  getFieldsDeep,
  getFuncsByFieldName,
} from 'shared/reducers/helpers/fields'

export function editRawText(query, rawText) {
  return Object.assign({}, query, {rawText})
}

export const chooseNamespace = (query, namespace, isKapacitorRule = false) => ({
  ...defaultQueryConfig({id: query.id, isKapacitorRule}),
  ...namespace,
})

export const chooseMeasurement = (
  query,
  measurement,
  isKapacitorRule = false
) => ({
  ...defaultQueryConfig({id: query.id, isKapacitorRule}),
  database: query.database,
  retentionPolicy: query.retentionPolicy,
  measurement,
})

export const toggleKapaField = (query, {name}) => {
  return {
    ...query,
    fields: [{name, type: 'field'}],
  }
}

export const buildInitialField = name => [
  {
    type: 'func',
    alias: `mean_${name}`,
    args: [{name, type: 'field'}],
    name: 'mean',
  },
]

export const addInitialField = (query, field, groupBy) => {
  return {
    ...query,
    fields: buildInitialField(field.name),
    groupBy,
  }
}

export const toggleField = (query, {name}) => {
  const {fields, groupBy} = query
  const defaultField = {
    type: 'func',
    alias: `mean_${name}`,
    args: [{name, type: 'field'}],
    name: 'mean',
  }

  if (!fields || !fields.length) {
    return {
      ...query,
      fields: [defaultField],
      groupBy: {...query.groupBy, time: '10s'},
    }
  }

  const isSelected = hasField(name, fields)
  const newFuncs = fields.filter(f => f.type === 'func')

  if (isSelected) {
    // if list is all fields, remove that field
    // if list is all funcs, remove all funcs that match
    const newFields = removeField(name, fields)
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
      fields: [...fields, {name, type: 'field'}],
    }
  }

  return {
    ...query,
    fields: [...fields, defaultField],
  }
}

/*
// all fields implicitly have a function applied to them, so consequently
// we need to set the auto group by time
export const toggleFieldWithGroupByInterval = (
  query,
  fieldFunc,
  isKapacitorRule
) => {

  const queryWithField = toggleField(query, fieldFunc, isKapacitorRule)
  return groupByTime(queryWithField, DEFAULT_DASHBOARD_GROUP_BY_INTERVAL)
}
*/

export function groupByTime(query, time) {
  return Object.assign({}, query, {
    groupBy: Object.assign({}, query.groupBy, {
      time,
    }),
  })
}

export const fill = (query, value) => ({...query, fill: value})

export function toggleTagAcceptance(query) {
  return Object.assign({}, query, {
    areTagsAccepted: !query.areTagsAccepted,
  })
}

export const removeFuncs = fields => getFieldsDeep(fields)

export const applyFuncsToField = (query, {field, funcs = []}, time) => {
  const nextFields = query.fields.reduce((acc, f) => {
    // If there is a func applied to only one field, add it to the other fields
    if (f.type === 'field') {
      return [
        ...acc,
        funcs.map(func => {
          const {name, type} = func
          const args = [{name: f.name, type: 'field'}]
          const alias = func.alias ? func.alias : `${func.name}_${f.name}`

          return {
            name,
            type,
            args,
            alias,
          }
        }),
      ]
    }

    const fieldToChange = f.args.find(a => a.name === field.name)

    // Apply new funcs to field
    if (fieldToChange) {
      const newFuncs = funcs.reduce((acc2, func) => {
        const funcsToChange = getFuncsByFieldName(fieldToChange.name, acc)
        const dup = funcsToChange.find(a => a.name === func.name)

        if (dup) {
          return acc2
        }

        return [
          ...acc2,
          {
            ...func,
            args: [field],
            alias: `${func.name}_${field.name}`,
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
    groupBy: {...query.groupBy, time},
  }
}

export function updateRawQuery(query, rawText) {
  return Object.assign({}, query, {
    rawText,
  })
}

export function groupByTag(query, tagKey) {
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

  return Object.assign({}, query, {
    groupBy: Object.assign({}, query.groupBy, {tags: newTags}),
  })
}

export function chooseTag(query, tag) {
  const tagValues = query.tags[tag.key]
  const shouldRemoveTag =
    tagValues && tagValues.length === 1 && tagValues[0] === tag.value
  if (shouldRemoveTag) {
    const newTags = Object.assign({}, query.tags)
    delete newTags[tag.key]
    return Object.assign({}, query, {tags: newTags})
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

  function updateTagValues(newTagValues) {
    return Object.assign({}, query, {
      tags: Object.assign({}, query.tags, {
        [tag.key]: newTagValues,
      }),
    })
  }
}
