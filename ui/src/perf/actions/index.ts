export const SET_SCROLL = 'SET_SCROLL'
export const SET_CELL_MOUNT = 'SET_CELL_MOUNT'
export const SET_DASHBOARD_VISIT = 'SET_DASHBOARD_VISIT'

export type Action =
  | ReturnType<typeof setScroll>
  | ReturnType<typeof setCellMount>
  | ReturnType<typeof setDashboardVisit>

export type ComponentKey = 'dashboard'
export type ScrollState = 'not scrolled' | 'scrolled'

export const setScroll = (component: ComponentKey, scroll: ScrollState) =>
  ({
    type: SET_SCROLL,
    component,
    scroll,
  } as const)

export const setCellMount = (cellID: string, mountStartMs: number) =>
  ({
    type: SET_CELL_MOUNT,
    cellID,
    mountStartMs,
  } as const)

export const setDashboardVisit = (dashboardID: string, startVisitMs: number) =>
  ({
    type: SET_DASHBOARD_VISIT,
    dashboardID,
    startVisitMs,
  } as const)
