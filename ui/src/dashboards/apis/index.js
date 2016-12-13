import AJAX from 'utils/ajax';

export function getDashboards() {
  return AJAX({
    method: 'GET',
    url: `/chronograf/v1/dashboards`,
  });
}
