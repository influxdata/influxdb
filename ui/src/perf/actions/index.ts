export const SET_RENDER_ID = 'SET_RENDER_ID'
export const SET_SCROLL = 'SET_SCROLL'
export const SET_CELL_MOUNT = 'SET_CELL_MOUNT'

export type Action =
  | ReturnType<typeof setRenderID>
  | ReturnType<typeof setScroll>
  | ReturnType<typeof setCellMount>

export type ComponentKey = 'dashboard'
export type ScrollState = 'not scrolled' | 'scrolled'

export const setRenderID = (component: ComponentKey, renderID: string) =>
  ({
    type: SET_RENDER_ID,
    component,
    renderID,
  } as const)

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
