// Libraries
import React, {SFC, ReactChildren} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import {AppWrapper} from '@influxdata/clockface'
import Nav from 'src/pageLayout'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/components/notifications/Notifications'
import OverlayController from 'src/overlays/components/OverlayController'
import CloudNav from 'src/pageLayout/components/CloudNav'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Types
import {AppState} from 'src/types'

interface StateProps {
  inPresentationMode: boolean
  dashboardLightMode: boolean
}
interface OwnProps {
  children: ReactChildren
}

type Props = OwnProps & StateProps & WithRouterProps

const App: SFC<Props> = ({
  children,
  inPresentationMode,
  location,
  dashboardLightMode,
}) => {
  const isViewingDashboard = location.pathname.includes('dashboards')
  const appWrapperClass = classnames('', {
    'dashboard-light-mode': isViewingDashboard && dashboardLightMode,
  })

  return (
    <>
      <CloudOnly>
        <CloudNav />
      </CloudOnly>
      <AppWrapper
        presentationMode={inPresentationMode}
        className={appWrapperClass}
      >
        <Notifications />
        <TooltipPortal />
        <NotesPortal />
        <OverlayController />
        <Nav />
        {children}
      </AppWrapper>
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {dashboardLightMode},
    },
  } = state

  return {inPresentationMode, dashboardLightMode}
}

export default connect<StateProps, {}>(
  mstp,
  null
)(withRouter(App))
