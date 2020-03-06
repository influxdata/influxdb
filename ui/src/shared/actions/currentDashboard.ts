export type Action = ReturnType<typeof setDashboard>

export const SET_CURRENT_DASHBOARD = 'SET_CURRENT_DASHBOARD'

export const setDashboard = (id?: string) =>
  ({
    type: SET_CURRENT_DASHBOARD,
    id,
  } as const)
