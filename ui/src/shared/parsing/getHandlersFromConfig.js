import _ from 'lodash'
import {ALERTS_FROM_CONFIG, CONFIG_TO_RULE} from 'src/kapacitor/constants'

const getHandlersFromConfig = config => {
  const {data: {sections}} = config

  const allAlerts = _.map(sections, (v, k) => {
    const fromConfig = _.get(v, ['elements', '0', 'options'], {})
    return {type: k, ...fromConfig}
  })

  const allowedAlerts = _.filter(allAlerts, a => a.type in ALERTS_FROM_CONFIG)

  const pickedAlerts = _.map(allowedAlerts, a => {
    return _.pick(a, ['type', 'enabled', ...ALERTS_FROM_CONFIG[a.type]])
  })

  const mappedAlerts = _.map(pickedAlerts, p => {
    return _.mapKeys(p, (v, k) => {
      return _.get(CONFIG_TO_RULE[p.type], k, k)
    })
  })
  return mappedAlerts
}

export default getHandlersFromConfig
