import _ from 'lodash'
import {HANDLERS_TO_RULE} from 'src/kapacitor/constants'

export const parseHandlersFromRule = (rule, handlersFromConfig) => {
  const handlersOfKind = {}
  const handlersOnThisAlert = []

  const handlersFromRule = _.pickBy(rule.alertNodes, (v, k) => {
    return k in HANDLERS_TO_RULE
  })

  _.forEach(handlersFromRule, (v, alertKind) => {
    const thisAlertFromConfig = _.find(
      handlersFromConfig,
      h => h.type === alertKind
    )

    _.forEach(v, alertOptions => {
      const count = _.get(handlersOfKind, alertKind, 0) + 1
      handlersOfKind[alertKind] = count

      if (alertKind === 'post') {
        const headers = alertOptions.headers
        alertOptions.headerKey = _.keys(headers)[0]
        alertOptions.headerValue = _.values(headers)[0]
        alertOptions = _.omit(alertOptions, 'headers')
      }

      const ep = {
        enabled: true,
        ...thisAlertFromConfig,
        ...alertOptions,
        alias: `${alertKind}-${count}`,
        type: alertKind,
      }
      handlersOnThisAlert.push(ep)
    })
  })
  const selectedHandler = handlersOnThisAlert.length
    ? handlersOnThisAlert[0]
    : null
  return {handlersOnThisAlert, selectedHandler, handlersOfKind}
}

export const parseAlertNodeList = rule => {
  const nodeList = _.transform(
    rule.alertNodes,
    (acc, v, k) => {
      if (k in HANDLERS_TO_RULE && v.length > 0) {
        if (k === 'slack') {
          _.reduce(
            v,
            (alerts, alert) => {
              const nickname = _.get(alert, 'workspace') || 'default'
              if (!alerts[nickname]) {
                const fullHandler = `${k} (${nickname})`
                alerts[nickname] = fullHandler
                acc.push(fullHandler)
              }
              return alerts
            },
            {}
          )
        } else {
          acc.push(k)
        }
      }
    },
    []
  )
  const uniqNodeList = _.uniq(nodeList)
  return _.join(uniqNodeList, ', ')
}
