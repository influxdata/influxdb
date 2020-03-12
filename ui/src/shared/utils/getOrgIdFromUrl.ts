export const getOrgIDFromUrl = function getOrgIDFromUrl() {
  // split urls like orgs/1337/dashboards
  const baseOrgID = window.location.pathname.split('orgs/')[1]
  // baseOrgID might have other paths behind it, like 1337/alerting
  let orgID = '-1'
  if (baseOrgID) {
    [orgID] = baseOrgID.split('/')
  }

  return orgID
}
