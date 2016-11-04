import AJAX from 'utils/ajax';

export function createRule(kapacitor, rule) {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.rules,
    data: rule,
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
    data: rule,
  });
}
