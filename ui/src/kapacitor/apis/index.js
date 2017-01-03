import AJAX from 'utils/ajax';

function rangeRule(rule) {
  const {value, rangeValue} = rule.values;

  if (rule.values.operator === 'within range') {
    rule.values.operator = 'less than';
    rule.values.rangeOperator = 'greater than';
    rule.values.value = Math.min(value, rangeValue).toString();
    rule.values.rangeValue = Math.max(value, rangeValue).toString();
  }

  if (rule.values.operator === 'out of range') {
    rule.values.operator = 'greater than';
    rule.values.rangeOperator = 'less than';
    rule.values.value = Math.max(value, rangeValue).toString();
    rule.values.rangeValue = Math.min(value, rangeValue).toString();
  }

  return rule;
}

export function createRule(kapacitor, rule) {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.rules,
    data: rangeRule(rule),
  });
}

export function getRules(kapacitor) {
  return AJAX({
    method: 'GET',
    url: kapacitor.links.rules,
  });
}

export function getRule(kapacitor, ruleID) {
  return AJAX({
    method: 'GET',
    url: `${kapacitor.links.rules}/${ruleID}`,
  });
}

export function editRule(rule) {
  return AJAX({
    method: 'PUT',
    url: rule.links.self,
    data: rangeRule(rule),
  });
}

export function deleteRule(rule) {
  return AJAX({
    method: 'DELETE',
    url: rule.links.self,
  });
}
