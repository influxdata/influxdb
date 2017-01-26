import AJAX from 'utils/ajax';

export function getDashboards() {
  return AJAX({
    method: 'GET',
    url: `/chronograf/v1/dashboards`,
  });
}

export function getDashboard(id) {
  return AJAX({
    method: 'GET',
    url: `/chronograf/v1/dashboards/${id}`,
  });
}
