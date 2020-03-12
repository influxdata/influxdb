export const getOrgIDFromUrl = function getOrgIDFromUrl() {
  // split urls like orgs/1337/dashboards
  const baseOrgID = window.location.pathname.split('orgs/')[1]
  // baseOrgID might have other paths behind it, like 1337/alerting

  if (baseOrgID) {
    return baseOrgID.split('/')[0]
  }

  return '-1'
}
