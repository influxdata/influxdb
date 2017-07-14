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

export const getRule = (kapacitor, ruleID) => {
  return AJAX({
    method: 'GET',
    url: `${kapacitor.links.rules}/${ruleID}`,
  })
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
