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

export function updateDashboardCell(cell) {
  return AJAX({
    method: 'PUT',
    url: cell.links.self,
    data: cell,
  })
}

export const createDashboard = async (dashboard) => {
  try {
    return await AJAX({
      method: 'POST',
      resource: 'dashboards',
      data: dashboard,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
