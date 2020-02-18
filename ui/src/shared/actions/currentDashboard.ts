export type Action = ReturnType<typeof setDashboard>

export const SET_DASHBOARD = 'SET_DASHBOARD_ID'

export const setDashboard = (id?: string) =>
  ({
    type: SET_DASHBOARD,
    id,
  } as const)
