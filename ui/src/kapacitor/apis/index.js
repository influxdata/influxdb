import AJAX from 'utils/ajax';

export function createRule(kapacitor, rule) {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.rules,
    data: rule,
  });
}
