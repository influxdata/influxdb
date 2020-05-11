export const fireOrgIdReady = function fireOrgIdReady(
  organizationIds: string[]
) {
  window.dataLayer = window.dataLayer || []
  window.dataLayer.push({
    event: 'cloudAppOrgIdReady',
    identity: {
      organizationIds,
    },
  })
}

export const fireUserDataReady = function fireUserDataReady(
  id: string,
  email: string
) {
  window.dataLayer = window.dataLayer || []
  window.dataLayer.push({
    event: 'cloudAppUserDataReady',
    identity: {
      email,
      id,
    },
  })
}

export const fireGAEvent = (event: string, payload: object = {}) => {
  window.dataLayer.push({
    event,
    ...payload,
  })
}

export const fireQueryEvent = (ownOrg: string, queryOrg: string) => {
  if (ownOrg === queryOrg) {
    fireGAEvent('orgData_queried')
  } else {
    fireGAEvent('demoData_queried')
  }
}
