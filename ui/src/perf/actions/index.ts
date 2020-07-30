export const SET_MOUNT_ID = 'SET_MOUNT_ID'
export const SET_SCROLL = 'SET_SCROLL'

export type Action =
  | ReturnType<typeof setMountID>
  | ReturnType<typeof setScroll>

export type ComponentKey = 'dashboard'
export type ScrollState = 'not scrolled' | 'scrolled'

export const setMountID = (component: ComponentKey, mountID: string) =>
  ({
    type: SET_MOUNT_ID,
    component,
    mountID,
  } as const)

export const setScroll = (component: ComponentKey, scroll: ScrollState) =>
  ({
    type: SET_SCROLL,
    component,
    scroll,
  } as const)
