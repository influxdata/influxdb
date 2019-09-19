// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay} from '@influxdata/clockface'

// Types
import {AppState, Organization} from 'src/types'

interface OwnProps {
  title: string
}

interface StateProps {
  org: Organization
}

type Props = OwnProps & StateProps & WithRouterProps

const ClientLibraryOverlay: FunctionComponent<Props> = ({
  title,
  children,
  router,
  org,
}) => {
  const onDismiss = () => {
    router.push(`/orgs/${org.id}/load-data/client-libraries`)
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

const mstp = ({orgs: {org}}: AppState): StateProps => ({
  org,
})

export default connect<StateProps>(mstp)(
  withRouter<OwnProps>(ClientLibraryOverlay)
)
