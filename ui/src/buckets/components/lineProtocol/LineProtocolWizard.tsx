// Libraries
import React, {useReducer, Dispatch} from 'react'
import {useHistory, useParams} from 'react-router-dom'
import {useSelector} from 'react-redux'

// Components
import {Overlay, OverlayFooter} from '@influxdata/clockface'
import LineProtocol from 'src/buckets/components/lineProtocol/configure/LineProtocol'
import {getByID} from 'src/resources/selectors'

// Actions
import {writeLineProtocolAction} from 'src/buckets/components/lineProtocol/LineProtocol.thunks'

// Reducers
import reducer, {
  initialState,
  LineProtocolState,
} from 'src/buckets/components/lineProtocol/LineProtocol.reducer'

// Types
import {ResourceType, AppState, Bucket} from 'src/types'
import {Action} from 'src/buckets/components/lineProtocol/LineProtocol.creators'
import LineProtocolFooterButtons from './LineProtocolFooterButtons'

// Selectors
import {getOrg} from 'src/organizations/selectors'

type LineProtocolContext = [LineProtocolState, Dispatch<Action>]
export const Context = React.createContext<LineProtocolContext>(null)

const getState = (bucketID: string) => (state: AppState) => {
  const bucket = getByID<Bucket>(state, ResourceType.Buckets, bucketID)
  const org = getOrg(state).name
  return {bucket: bucket?.name || '', org}
}

const LineProtocolWizard = () => {
  const history = useHistory()
  const {bucketID, orgID} = useParams()
  const {bucket, org} = useSelector(getState(bucketID))

  const [state, dispatch] = useReducer(reducer, initialState())
  const {body, precision} = state

  const handleDismiss = () => {
    history.push(`/orgs/${orgID}/load-data/buckets`)
  }

  const handleSubmit = () => {
    writeLineProtocolAction(dispatch, org, bucket, body, precision)
  }

  return (
    <Context.Provider value={[state, dispatch]}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Add Data Using Line Protocol"
            onDismiss={handleDismiss}
          />
          <LineProtocol onSubmit={handleSubmit} />
          <OverlayFooter>
            <LineProtocolFooterButtons onSubmit={handleSubmit} />
          </OverlayFooter>
        </Overlay.Container>
      </Overlay>
    </Context.Provider>
  )
}

export default LineProtocolWizard
