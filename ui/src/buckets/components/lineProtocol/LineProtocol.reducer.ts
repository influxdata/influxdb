import {RemoteDataState} from 'src/types'
import {WritePrecision} from '@influxdata/influx'
import {produce} from 'immer'

import {
  Action,
  SET_BODY,
  SET_PRECISION,
  SET_WRITE_STATUS,
  SET_TAB,
  RESET_LINE_PROTOCOL_STATE,
} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

export interface LineProtocolState {
  body: string
  tab: 'Upload File' | 'Enter Manually'
  writeStatus: RemoteDataState
  writeError: string
  precision: WritePrecision
}

export const initialState = (): LineProtocolState => ({
  body: '',
  tab: 'Upload File',
  writeStatus: RemoteDataState.NotStarted,
  writeError: '',
  precision: WritePrecision.Ns,
})

const lineProtocolReducer = (
  state = initialState(),
  action: Action
): LineProtocolState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_BODY: {
        draftState.body = action.body

        return
      }
      case SET_TAB: {
        draftState.tab = action.tab

        return
      }
      case SET_WRITE_STATUS: {
        draftState.writeStatus = action.writeStatus
        draftState.writeError = action.writeError

        return
      }
      case SET_PRECISION: {
        draftState.precision = action.precision

        return
      }
      case RESET_LINE_PROTOCOL_STATE: {
        draftState.body = ''
        draftState.writeStatus = RemoteDataState.NotStarted
        draftState.writeError = ''

        return
      }
    }
  })

export default lineProtocolReducer
