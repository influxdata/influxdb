// Libraries
import React, {useReducer, Dispatch} from 'react'
import {useHistory, useParams} from 'react-router-dom'
import {useSelector} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import LineProtocol from 'src/buckets/components/lineProtocol/configure/LineProtocol'
import {getByID} from 'src/resources/selectors'

// Reducers
import reducer, {
  initialState,
  LineProtocolState,
} from 'src/buckets/components/lineProtocol/LineProtocol.reducer'

// Types
import {ResourceType, AppState, Bucket} from 'src/types'
import {Action} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

type LineProtocolContext = [LineProtocolState, Dispatch<Action>]
export const Context = React.createContext<LineProtocolContext>(null)

const getState = (bucketID: string) => (state: AppState) => {
  const bucket = getByID<Bucket>(state, ResourceType.Buckets, bucketID)
  return bucket?.name || ''
}

const LineProtocolWizard = () => {
  const history = useHistory()
  const {bucketID} = useParams()
  const bucket = useSelector(getState(bucketID))
  const handleDismiss = () => {
    history.goBack()
  }

  const value = useReducer(reducer, initialState())

  return (
    <Context.Provider value={value}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Add Data Using Line Protocol"
            onDismiss={handleDismiss}
          />
          <LineProtocol bucket={bucket} />
        </Overlay.Container>
      </Overlay>
    </Context.Provider>
  )
}

export default LineProtocolWizard
