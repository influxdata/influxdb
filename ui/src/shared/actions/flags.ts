export const SET_FEATURE_FLAGS = 'SET_FEATURE_FLAGS'
export const CLEAR_FEATURE_FLAG_OVERRIDES = 'CLEAR_FEATURE_FLAG_OVERRIDES'
export const SET_FEATURE_FLAG_OVERRIDE = 'SET_FEATURE_FLAG_OVERRIDE'

export type Actions =
  | ReturnType<typeof setFlags>
  | ReturnType<typeof clearOverrides>
  | ReturnType<typeof setOverride>

// NOTE: this doesnt have a type as it will be determined
// by the backend at a later time and keeping the format
// open for transformations in a bit
export const setFlags = flags =>
  ({
    type: SET_FEATURE_FLAGS,
    payload: flags,
  } as const)

export const clearOverrides = () =>
  ({
    type: CLEAR_FEATURE_FLAG_OVERRIDES,
  } as const)

export const setOverride = (flag: string, value: string | boolean) =>
  ({
    type: SET_FEATURE_FLAG_OVERRIDE,
    payload: {
      [flag]: value,
    },
  } as const)
