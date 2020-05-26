import {DemoDataDashboardNames} from 'src/cloud/constants'

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

export const fireEvent = (event: string, payload: object = {}) => {
  window.dataLayer = window.dataLayer || []
  window.dataLayer.push({
    event,
    ...payload,
  })
}

export const fireQueryEvent = (ownOrg: string, queryOrg: string) => {
  if (ownOrg === queryOrg) {
    fireEvent('orgData_queried')
  } else {
    fireEvent('demoData_queried')
  }
}

export const fireDashboardViewedEvent = (dashboardName: string) => {
  const demoDataset = DemoDataDashboardNames[dashboardName]
  if (demoDataset) {
    fireEvent('demoData_dashboardViewed', {demo_dataset: demoDataset})
  }
}
