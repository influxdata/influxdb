import _ from 'lodash'

const getHandlersFromRule = rule => {
  const handlersOfKind = {}
  const handlersOnThisAlert = []

  _.forEach(rule.alertNodes, (v, k) => {
    const count = _.get(handlersOfKind, k, 0) + 1
    handlersOfKind[k] = count
    const ep = {
      ...v,
      alias: k + count,
      type: k,
    }
    handlersOnThisAlert.push(ep)
  })
  const selectedHandler = handlersOnThisAlert.length
    ? handlersOnThisAlert[0]
    : null
  return {handlersOnThisAlert, selectedHandler, handlersOfKind}
}

export default getHandlersFromRule
