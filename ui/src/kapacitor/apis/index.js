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
