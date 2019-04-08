// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Styles
import 'src/variables/components/CreateVariableOverlay.scss'

// Actions
import {createVariable} from 'src/variables/actions'

// Components
import {Overlay} from 'src/clockface'
import VariableForm from 'src/variables/components/VariableForm'

interface DispatchProps {
  onCreateVariable: typeof createVariable
}

type Props = DispatchProps & WithRouterProps

class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onCreateVariable} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={1000}>
          <Overlay.Heading
            title="Create Variable"
            onDismiss={this.handleHideOverlay}
          />
          <Overlay.Body>
            <VariableForm
              onCreateVariable={onCreateVariable}
              onHideOverlay={this.handleHideOverlay}
            />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleHideOverlay = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/variables`)
  }
}

const mdtp: DispatchProps = {
  onCreateVariable: createVariable,
}

export default connect<Props>(
  null,
  mdtp
)(withRouter<{}>(CreateVariableOverlay))
