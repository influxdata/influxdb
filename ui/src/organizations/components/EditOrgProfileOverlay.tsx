// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps, withRouter} from 'react-router'

import _ from 'lodash'

// Components
import {Overlay} from 'src/clockface'
import RenameOrgForm from 'src/organizations/components/RenameOrgForm'
import EditOrgConfirmationForm from 'src/organizations/components/EditOrgConfirmationForm'
import {ErrorHandling} from 'src/shared/decorators/errors'

type Props = WithRouterProps

interface State {
  isConfirmed: boolean
}

@ErrorHandling
class EditOrgProfileOverlay extends PureComponent<Props, State> {
  public state = {
    isConfirmed: false,
  }

  public render() {
    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={400}>
          <Overlay.Heading
            title={this.overlayTitle}
            onDismiss={this.handleCloseOverlay}
          />
          <Overlay.Body>{this.overlayContents}</Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get overlayTitle() {
    if (this.state.isConfirmed) {
      return 'Rename Organization'
    }

    return 'Are you sure?'
  }

  private get overlayContents() {
    if (this.state.isConfirmed) {
      return <RenameOrgForm />
    }

    return <EditOrgConfirmationForm onConfirm={this.handleConfirm} />
  }

  private handleCloseOverlay = () => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/profile`)
  }

  private handleConfirm = () => {
    this.setState({isConfirmed: true})
  }
}

export default withRouter(EditOrgProfileOverlay)
