import _ from 'lodash'

export default function parseShowTagValues(response) {
  // Currently only supports SHOW TAG VALUES responses that explicitly specify a measurement,
  // Ceaning we can safely work with just the first result from the response.
  const result = response.results[0]

  if (result.error) {
    return {errors: [], tags: []}
  }

  const tags = {}
  ;(result.series || []).forEach(({columns, values}) => {
    values.forEach(v => {
      const tagKey = v[columns.indexOf('key')]
      const tagValue = v[columns.indexOf('value')]

      if (!tags[tagKey]) {
        tags[tagKey] = []
      }

      tags[tagKey].push(tagValue)
    })
  })

  // uniqueness of tag values are no longer guaranteed see showTagValueSpec for example
  _.each(tags, (v, k) => {
    tags[k] = _.uniq(v).sort()
  })

  return {errors: [], tags}
}
