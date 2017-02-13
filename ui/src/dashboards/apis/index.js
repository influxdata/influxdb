import AJAX from 'utils/ajax';

export function getDashboards() {
  return AJAX({
    method: 'GET',
    resource: 'dashboards',
  });
}
