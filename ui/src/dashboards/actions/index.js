export function loadDashboards(dashboards) {
  return {
    type: 'LOAD_DASHBOARDS',
    payload: {
      dashboards,
    },
  }
}
