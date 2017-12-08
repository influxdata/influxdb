import _ from 'lodash'
import {
  ALERTS_FROM_CONFIG,
  MAP_FIELD_KEYS_FROM_CONFIG,
  MAP_KEYS_FROM_CONFIG,
} from 'src/kapacitor/constants'

const parseHandlersFromConfig = config => {
  const {data: {sections}} = config

  const allHandlers = _.map(sections, (v, k) => {
    const fromConfig = _.get(v, ['elements', '0', 'options'], {})
    return {
      // fill type with handler names in rule
      type: _.get(MAP_KEYS_FROM_CONFIG, k, k),
      ...fromConfig,
    }
  })

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
