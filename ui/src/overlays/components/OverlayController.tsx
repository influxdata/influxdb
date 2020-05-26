// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState} from 'src/types'
import {OverlayID} from 'src/overlays/reducers/overlays'

// Components
import {Overlay} from '@influxdata/clockface'
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'
import AllAccessTokenOverlay from 'src/authorizations/components/AllAccessTokenOverlay'
import BucketsTokenOverlay from 'src/authorizations/components/BucketsTokenOverlay'
import TelegrafConfigOverlay from 'src/telegrafs/components/TelegrafConfigOverlay'
import TelegrafOutputOverlay from 'src/telegrafs/components/TelegrafOutputOverlay'
import OrgSwitcherOverlay from 'src/pageLayout/components/OrgSwitcherOverlay'
import CreateBucketOverlay from 'src/buckets/components/CreateBucketOverlay'

// Actions
import {dismissOverlay} from 'src/overlays/actions/overlays'

interface StateProps {
  overlayID: OverlayID
  onClose: () => void
}

interface DispatchProps {
  clearOverlayControllerState: typeof dismissOverlay
}

type OverlayControllerProps = StateProps & DispatchProps

const OverlayController: FunctionComponent<OverlayControllerProps> = props => {
  let activeOverlay = <></>
  let visibility = true

  const {overlayID, onClose, clearOverlayControllerState} = props

  const closer = () => {
    clearOverlayControllerState()
    if (onClose) {
      onClose()
    }
  }

  switch (overlayID) {
    case 'add-note':
    case 'edit-note':
      activeOverlay = <NoteEditorOverlay onClose={closer} />
      break
    case 'add-master-token':
      activeOverlay = <AllAccessTokenOverlay onClose={closer} />
      break
    case 'add-token':
      activeOverlay = <BucketsTokenOverlay onClose={closer} />
      break
    case 'telegraf-config':
      activeOverlay = <TelegrafConfigOverlay onClose={closer} />
      break
    case 'telegraf-output':
      activeOverlay = <TelegrafOutputOverlay onClose={closer} />
      break
    case 'switch-organizations':
      activeOverlay = <OrgSwitcherOverlay onClose={closer} />
      break
    case 'create-bucket':
      activeOverlay = <CreateBucketOverlay onClose={closer} />
      break
    default:
      visibility = false
  }

  return <Overlay visible={visibility}>{activeOverlay}</Overlay>
}

const mstp = (state: AppState): StateProps => {
  const id = state.overlays.id
  const onClose = state.overlays.onClose

  return {
    overlayID: id,
    onClose,
  }
}

const mdtp = {
  clearOverlayControllerState: dismissOverlay,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(OverlayController)
