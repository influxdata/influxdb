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

export const createTask = async (kapacitor, {id, dbrps, script, type}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: kapacitor.links.tasks,
      data: {
        id,
        type,
        dbrps,
        script,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateTask = async (
  kapacitor,
  {id, dbrps, script, type},
  ruleID
) => {
  try {
    return await AJAX({
      method: 'PATCH',
      url: `${kapacitor.links.tasks}/${ruleID}`,
      data: {
        id,
        type,
        dbrps,
        script,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
