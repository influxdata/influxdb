// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, Overlay} from 'src/clockface'
import VariableRow from 'src/organizations/components/VariableRow'
import UpdateVariableOverlay from 'src/organizations/components/UpdateVariableOverlay'

// Types
import {Variable} from '@influxdata/influx'
import {OverlayState} from 'src/types'

interface Props {
  variables: Variable[]
  emptyState: JSX.Element
  onDeleteVariable: (variable: Variable) => void
  onUpdateVariable: (variable: Variable) => void
}

interface State {
  variableID: string
  variableOverlayState: OverlayState
}

class VariableList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      variableID: null,
      variableOverlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {
      emptyState,
      variables,
      onDeleteVariable,
      onUpdateVariable,
    } = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="60%" />
            <IndexList.HeaderCell columnName="Type" width="40%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {variables.map(variable => (
              <VariableRow
                key={`variable-${variable.id}`}
                variable={variable}
                onDeleteVariable={onDeleteVariable}
                onUpdateVariableName={onUpdateVariable}
                onEditVariable={this.handleStartEdit}
              />
            ))}
          </IndexList.Body>
        </IndexList>
        <Overlay visible={this.isVariableOverlayVisible}>
          <UpdateVariableOverlay
            variable={this.variable}
            onCloseOverlay={this.handleCloseOverlay}
            onUpdateVariable={this.handleUpdateVariable}
          />
        </Overlay>
      </>
    )
  }

  private get variable(): Variable {
    return this.props.variables.find(v => v.id === this.state.variableID)
  }

  private get isVariableOverlayVisible(): boolean {
    return this.state.variableOverlayState === OverlayState.Open
  }

  private handleCloseOverlay = () => {
    this.setState({variableOverlayState: OverlayState.Closed, variableID: null})
  }

  private handleStartEdit = (variable: Variable) => {
    this.setState({
      variableID: variable.id,
      variableOverlayState: OverlayState.Open,
    })
  }

  private handleUpdateVariable = async (variable: Variable): Promise<void> => {
    this.props.onUpdateVariable(variable)
  }
}

export default VariableList
