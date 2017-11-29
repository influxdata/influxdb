import _ from 'lodash'
import {
  ALERTS_FROM_CONFIG,
  MAP_FIELD_KEYS_FROM_CONFIG,
  MAP_KEYS_FROM_CONFIG,
} from 'src/kapacitor/constants'

const getHandlersFromConfig = config => {
  const {data: {sections}} = config

  const allHandlers = _.map(sections, (v, k) => {
    const fromConfig = _.get(v, ['elements', '0', 'options'], {})
    return {
      type: _.get(MAP_KEYS_FROM_CONFIG, k, k),
      ...fromConfig,
    }
  })

  const mappedHandlers = _.mapKeys(allHandlers, (v, k) => {
    return _.get(MAP_KEYS_FROM_CONFIG, k, k)
  })

  const allowedHandlers = _.filter(
    mappedHandlers,
    h => h.type in ALERTS_FROM_CONFIG
  )

  const pickedHandlers = _.map(allowedHandlers, h => {
    return _.pick(h, ['type', 'enabled', ...ALERTS_FROM_CONFIG[h.type]])
  })

  const fieldKeyMappedHandlers = _.map(pickedHandlers, h => {
    return _.mapKeys(h, (v, k) => {
      return _.get(MAP_FIELD_KEYS_FROM_CONFIG[h.type], k, k)
    })
  })
  return fieldKeyMappedHandlers
}

export default getHandlersFromConfig
