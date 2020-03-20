// Libraries
import React, {SFC, ReactChildren} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import {AppWrapper} from '@influxdata/clockface'
import TreeNav from 'src/pageLayout/containers/TreeNav'
import Nav from 'src/pageLayout/containers/Nav'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/components/notifications/Notifications'
import OverlayController from 'src/overlays/components/OverlayController'
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import CloudNav from 'src/pageLayout/components/CloudNav'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Types
import {AppState, CurrentPage, Theme} from 'src/types'

interface StateProps {
  inPresentationMode: boolean
  currentPage: CurrentPage
  theme: Theme
}
interface OwnProps {
  children: ReactChildren
}

type Props = OwnProps & StateProps & WithRouterProps

const App: SFC<Props> = ({
  children,
  inPresentationMode,
  currentPage,
  theme,
}) => {
  const appWrapperClass = classnames('', {
    'dashboard-light-mode': currentPage === 'dashboard' && theme === 'light',
  })

  return (
    <>
      <FeatureFlag name="treeNav" equals={false}>
        <CloudOnly>
          <CloudNav />
        </CloudOnly>
      </FeatureFlag>
      <AppWrapper
        presentationMode={inPresentationMode}
        className={appWrapperClass}
      >
        <Notifications />
        <TooltipPortal />
        <NotesPortal />
        <OverlayController />
        <FeatureFlag name="treeNav">
          <TreeNav />
        </FeatureFlag>
        <FeatureFlag name="treeNav" equals={false}>
          <Nav />
        </FeatureFlag>
        {children}
      </AppWrapper>
    </>
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

export default connect<StateProps, {}>(
  mstp,
  null
)(withRouter(App))
