import AJAX from 'utils/ajax'

const rangeRule = rule => {
  const {value, rangeValue, operator} = rule.values

  if (operator === 'inside range' || operator === 'outside range') {
    rule.values.value = Math.min(value, rangeValue).toString()
    rule.values.rangeValue = Math.max(value, rangeValue).toString()
  }

  return rule
}

export const createRule = (kapacitor, rule) => {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.rules,
    data: rangeRule(rule),
  })
}

export const getRules = kapacitor => {
  return AJAX({
    method: 'GET',
    url: kapacitor.links.rules,
  })
}

export const getRule = async (kapacitor, ruleID) => {
  try {
    return await AJAX({
      method: 'GET',
      url: `${kapacitor.links.rules}/${ruleID}`,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const editRule = rule => {
  return AJAX({
    method: 'PUT',
    url: rule.links.self,
    data: rangeRule(rule),
  })
}

export const deleteRule = rule => {
  return AJAX({
    method: 'DELETE',
    url: rule.links.self,
  })
}

export const updateRuleStatus = (rule, status) => {
  return AJAX({
    method: 'PATCH',
    url: rule.links.self,
    data: {status},
  })
}

export const createTask = async (kapacitor, {id, dbrps, tickscript, type}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: kapacitor.links.rules,
      data: {
        id,
        type,
        dbrps,
        tickscript,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateTask = async (
  kapacitor,
  {id, dbrps, tickscript, type},
  ruleID
) => {
  try {
    return await AJAX({
      method: 'PUT',
      url: `${kapacitor.links.rules}/${ruleID}`,
      data: {
        id,
        type,
        dbrps,
        tickscript,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

const kapacitorLogHeaders = {
  'Content-Type': 'application/json',
  Accept: 'application/json',
}

export const getLogStream = kapacitor =>
  fetch(`${kapacitor.links.proxy}?path=/kapacitor/v1preview/logs`, {
    method: 'GET',
    headers: kapacitorLogHeaders,
    credentials: 'include',
  })

export const getLogStreamByRuleID = (kapacitor, ruleID) =>
  fetch(
    `${kapacitor.links.proxy}?path=/kapacitor/v1preview/logs?task=${ruleID}`,
    {
      method: 'GET',
      headers: kapacitorLogHeaders,
      credentials: 'include',
    }
  )

export const pingKapacitorVersion = async kapacitor => {
  try {
    const result = await AJAX({
      method: 'GET',
      url: `${kapacitor.links.proxy}?path=/kapacitor/v1preview/ping`,
      headers: kapacitorLogHeaders,
      credentials: 'include',
    })
    const kapVersion = result.headers['x-kapacitor-version']
    return kapVersion === '' ? null : kapVersion
  } catch (error) {
    console.error(error)
    throw error
  }
}
