import AJAX from 'utils/ajax';

export function getDashboards() {
  return AJAX({
    method: 'GET',
    resource: 'dashboards',
  });
}

export function updateDashboard(dashboard) {
  return AJAX({
    method: 'PUT',
    url: dashboard.links.self,
    data: dashboard,
  });
}
