// Libraries
import React, {SFC, ReactChildren} from 'react'
import {connect} from 'react-redux'

// Components
import {AppWrapper} from '@influxdata/clockface'
import Nav from 'src/pageLayout'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/components/notifications/Notifications'
import OverlayController from 'src/overlays/components/OverlayController'

// Types
import {AppState} from 'src/types'

interface StateProps {
  inPresentationMode: boolean
}
interface OwnProps {
  children: ReactChildren
}

type Props = OwnProps & StateProps

const App: SFC<Props> = ({children, inPresentationMode}) => (
  <AppWrapper presentationMode={inPresentationMode}>
    <Notifications />
    <TooltipPortal />
    <NotesPortal />
    <OverlayController />
    <Nav />
    {children}
  </AppWrapper>
)

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      ephemeral: {inPresentationMode},
    },
  } = state

  return {inPresentationMode}
}

export default connect<StateProps, {}>(
  mstp,
  null
)(App)
