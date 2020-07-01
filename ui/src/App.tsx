// Libraries
import React, {SFC} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import classnames from 'classnames'
import {Switch, Route} from 'react-router-dom'

// Components
import {AppWrapper} from '@influxdata/clockface'
import TreeNav from 'src/pageLayout/containers/TreeNav'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/components/notifications/Notifications'
import OverlayController from 'src/overlays/components/OverlayController'
import SetOrg from 'src/shared/containers/SetOrg'

// Types
import {AppState, CurrentPage, Theme} from 'src/types'
import CreateOrgOverlay from './organizations/components/CreateOrgOverlay'

interface StateProps {
  inPresentationMode: boolean
  currentPage: CurrentPage
  theme: Theme
}
type Props = StateProps & RouteComponentProps

const App: SFC<Props> = ({inPresentationMode, currentPage, theme}) => {
  const appWrapperClass = classnames('', {
    'dashboard-light-mode': currentPage === 'dashboard' && theme === 'light',
  })

  return (
    <AppWrapper
      presentationMode={inPresentationMode}
      className={appWrapperClass}
    >
      <Notifications />
      <TooltipPortal />
      <NotesPortal />
      <OverlayController />
      <TreeNav />
      <Switch>
        <Route path="/orgs/new" component={CreateOrgOverlay} />
        <Route path="/orgs/:orgID" component={SetOrg} />
      </Switch>
    </AppWrapper>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {theme},
    },
    currentPage,
  } = state

  return {inPresentationMode, currentPage, theme}
}

export default connect<StateProps, {}>(mstp, null)(withRouter(App))
