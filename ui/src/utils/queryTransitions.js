import defaultQueryConfig from './defaultQueryConfig'

export function editRawText(query, rawText) {
  return Object.assign({}, query, {rawText})
}

export function chooseNamespace(query, namespace) {
  return Object.assign({}, defaultQueryConfig(query.id), namespace)
}

export function chooseMeasurement(query, measurement) {
  return Object.assign({}, defaultQueryConfig(query.id), {
    database: query.database,
    retentionPolicy: query.retentionPolicy,
    measurement,
  })
}

export function toggleField(query, {field, funcs}, isKapacitorRule = false) {
  const isSelected = query.fields.find(f => f.field === field)
  if (isSelected) {
    const nextFields = query.fields.filter(f => f.field !== field)
    if (!nextFields.length) {
      const nextGroupBy = Object.assign({}, query.groupBy, {time: null})
      return Object.assign({}, query, {
        fields: nextFields,
        groupBy: nextGroupBy,
      })
    }

    return Object.assign({}, query, {
      fields: nextFields,
    })
  }

  if (isKapacitorRule) {
    return Object.assign({}, query, {
      fields: [{field, funcs}],
    })
  }

  const hasFuncs = query.fields.find(f => f.funcs.length)
  let newFuncs = []
  if (hasFuncs) {
    newFuncs = hasFuncs.funcs
  }

  return {
    ...query,
    fields: query.fields.concat({
      field,
      funcs: newFuncs,
    }),
  }
}

export function groupByTime(query, time) {
  return Object.assign({}, query, {
    groupBy: Object.assign({}, query.groupBy, {
      time,
    }),
  })
}

export function toggleTagAcceptance(query) {
  return Object.assign({}, query, {
    areTagsAccepted: !query.areTagsAccepted,
  })
}

export function applyFuncsToField(query, {field, funcs}) {
  const shouldRemoveFuncs = funcs.length === 0
  const nextFields = query.fields.map(f => {
    // If one field has no funcs, all fields must have no funcs
    if (shouldRemoveFuncs) {
      return Object.assign({}, f, {funcs: []})
    }

    // If there is a func applied to only one field, add it to the other fields
    if (f.field === field || !f.funcs || !f.funcs.length) {
      return Object.assign({}, f, {funcs})
    }

    return f
  })

  // If there are no functions, then there should be no GROUP BY time
  if (shouldRemoveFuncs) {
    const nextGroupBy = Object.assign({}, query.groupBy, {time: null})
    return Object.assign({}, query, {
      fields: nextFields,
      groupBy: nextGroupBy,
    })
  }

  return Object.assign({}, query, {fields: nextFields})
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
