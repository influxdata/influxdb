// Types
import {RemoteDataState, WritePrecision, LineProtocolTab} from 'src/types'

export const SET_BODY = 'SET_BODY'
export const SET_TAB = 'SET_TAB'
export const SET_WRITE_STATUS = 'SET_WRITE_STATUS'
export const SET_PRECISION = 'SET_PRECISION'

export type Action =
  | ReturnType<typeof setBody>
  | ReturnType<typeof setTab>
  | ReturnType<typeof setWriteStatus>
  | ReturnType<typeof setPrecision>

export const setBody = (body: string) =>
  ({
    type: SET_BODY,
    body,
  } as const)

export const setTab = (tab: LineProtocolTab) =>
  ({
    type: SET_TAB,
    tab,
  } as const)

export const setWriteStatus = (writeStatus: RemoteDataState, writeError = '') =>
  ({
    type: SET_WRITE_STATUS,
    writeStatus,
    writeError,
  } as const)

export const setPrecision = (precision: WritePrecision) =>
  ({
    type: SET_PRECISION,
    precision,
  } as const)
