import _ from 'lodash'
import {ALERTS_TO_RULE} from 'src/kapacitor/constants'

export const getHandlersFromRule = rule => {
  const handlersOfKind = {}
  const handlersOnThisAlert = []
  const handlersFromRule = _.pickBy(rule.alertNodes, (v, k) => {
    return k in ALERTS_TO_RULE
  })

  _.forEach(handlersFromRule, (v, alertKind) => {
    if (v.length > 0) {
      _.forEach(v, alertOptions => {
        const count = _.get(handlersOfKind, alertKind, 0) + 1
        handlersOfKind[alertKind] = count
        const ep = {
          ...alertOptions,
          alias: alertKind + count,
          type: alertKind,
          enabled: true,
        }
        handlersOnThisAlert.push(ep)
      })
    }
  })
  const selectedHandler = handlersOnThisAlert.length
    ? handlersOnThisAlert[0]
    : null
  return {handlersOnThisAlert, selectedHandler, handlersOfKind}
}

export const getAlertNodeList = rule => {
  const nodeList = _.transform(
    rule.alertNodes,
    (acc, v, k) => {
      if (k in ALERTS_TO_RULE && v.length > 0) {
        acc.push(k)
      }
    },
    []
  )
  const uniqNodeList = _.uniq(nodeList)
  return _.join(uniqNodeList, ', ')
}
