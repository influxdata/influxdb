// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Utils
import {extractVariablesList} from 'src/variables/selectors'

// Actions
import {createVariable} from 'src/variables/actions'

// Components
import {Overlay} from '@influxdata/clockface'
import VariableForm from 'src/variables/components/VariableForm'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Types
import {AppState} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'

interface DispatchProps {
  onCreateVariable: typeof createVariable
}

interface StateProps {
  variables: Variable[]
}

type Props = DispatchProps & WithRouterProps & StateProps

class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onCreateVariable, variables} = this.props

    return (
      <GetResources resource={ResourceTypes.Variables}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={1000}>
            <Overlay.Header
              title="Create Variable"
              onDismiss={this.handleHideOverlay}
            />
            <Overlay.Body>
              <VariableForm
                variables={variables}
                onCreateVariable={onCreateVariable}
                onHideOverlay={this.handleHideOverlay}
              />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </GetResources>
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

const mstp = (state: AppState): StateProps => {
  const variables = extractVariablesList(state)

  return {variables}
}

const mdtp: DispatchProps = {
  onCreateVariable: createVariable,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<{}>(CreateVariableOverlay))
