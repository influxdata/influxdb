// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Utils
import {extractVariablesList} from 'src/variables/selectors'

// Actions
import {createVariable} from 'src/variables/actions'

// Components
import {Overlay} from '@influxdata/clockface'
import VariableForm from 'src/variables/components/VariableForm'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Types
import {AppState} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'

interface DispatchProps {
  onCreateVariable: typeof createVariable
}

interface StateProps {
  variables: Variable[]
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & DispatchProps & StateProps

class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onCreateVariable, variables, onDismiss} = this.props

    return (
      <GetResources resource={ResourceType.Variables}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={1000}>
            <Overlay.Header title="Create Variable" onDismiss={onDismiss} />
            <Overlay.Body>
              <VariableForm
                variables={variables}
                onCreateVariable={onCreateVariable}
                onHideOverlay={onDismiss}
              />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </GetResources>
    )
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
)(CreateVariableOverlay)
