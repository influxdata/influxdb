// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Overlay} from '@influxdata/clockface'

// Types
import {AppState, Organization} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface OwnProps {
  title: string
}

interface StateProps {
  org: Organization
}

type Props = OwnProps & StateProps & RouteComponentProps<{orgID: string}>

const ClientLibraryOverlay: FunctionComponent<Props> = ({
  title,
  children,
  history,
  org,
}) => {
  const onDismiss = () => {
    history.push(`/orgs/${org.id}/load-data/client-libraries`)
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={980}>
        <Overlay.Header title={title} onDismiss={onDismiss} />
        <Overlay.Body className="client-library-overlay">
          {children}
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => ({
  org: getOrg(state),
})

export default connect<StateProps>(mstp)(withRouter(ClientLibraryOverlay))
