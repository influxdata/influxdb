import AJAX from 'utils/ajax';

function rangeRule(rule) {
  if (rule.values.operator === 'within range') {
    const {value, rangeValue} = rule.values;
    rule.values.operator = 'less than';
    rule.values.rangeOperator = 'greater than';
    rule.values.value = Math.min(value, rangeValue).toString();
    rule.values.rangeValue = Math.max(value, rangeValue).toString();
  }
  console.log(rule.values);
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
