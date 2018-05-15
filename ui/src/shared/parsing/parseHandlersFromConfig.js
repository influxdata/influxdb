import _ from 'lodash'
import {
  ALERTS_FROM_CONFIG,
  MAP_FIELD_KEYS_FROM_CONFIG,
  MAP_KEYS_FROM_CONFIG,
} from 'src/kapacitor/constants'

const getElementOptions = section => {
  const elements = _.get(section, 'elements', [])

  if (elements.length === 0) {
    return {}
  }

  return _.map(elements, element => _.get(element, 'options', {}))
}

const parseHandlersFromConfig = config => {
  const {
    data: {sections},
  } = config

  const allHandlers = _.reduce(
    sections,
    (acc, v, k) => {
      const options = getElementOptions(v)
      const type = _.get(MAP_KEYS_FROM_CONFIG, k, k)

      _.forEach(options, option => {
        acc.push({
          // fill type with handler names in rule
          type,
          ...option,
        })
      })
      return acc
    },
    []
  )

  // map handler names from config to handler names in rule
  const mappedHandlers = _.mapKeys(allHandlers, (v, k) => {
    return _.get(MAP_KEYS_FROM_CONFIG, k, k)
  })

  // filter out any handlers from config that are not allowed
  const allowedHandlers = _.filter(
    mappedHandlers,
    h => h.type in ALERTS_FROM_CONFIG
  )

  // filter out any fields of handlers that are not allowed
  const pickedHandlers = _.map(allowedHandlers, h => {
    return _.pick(h, ['type', 'enabled', ...ALERTS_FROM_CONFIG[h.type]])
  })

  // map field names from config to field names in rule
  const fieldKeyMappedHandlers = _.map(pickedHandlers, h => {
    return _.mapKeys(h, (v, k) => {
      return _.get(MAP_FIELD_KEYS_FROM_CONFIG[h.type], k, k)
    })
  })

  return fieldKeyMappedHandlers
}

export default parseHandlersFromConfig
