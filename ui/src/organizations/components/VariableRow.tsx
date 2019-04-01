// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {IndexList, Alignment, Context, IconFont} from 'src/clockface'
import {ComponentColor} from '@influxdata/clockface'

// Types
import {Variable} from '@influxdata/influx'
import EditableName from 'src/shared/components/EditableName'

interface OwnProps {
  variable: Variable
  onDeleteVariable: (variable: Variable) => void
  onUpdateVariableName: (variable: Partial<Variable>) => void
  onEditVariable: (variable: Variable) => void
}

type Props = OwnProps & WithRouterProps

class VariableRow extends PureComponent<Props> {
  public render() {
    const {variable, onDeleteVariable} = this.props

    return (
      <IndexList.Row testID="variable-row">
        <IndexList.Cell alignment={Alignment.Left}>
          <EditableName
            onUpdate={this.handleUpdateVariableName}
            name={variable.name}
            noNameString="NAME THIS VARIABLE"
            onEditName={this.handleEditVariable}
          >
            {variable.name}
          </EditableName>
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Left}>Query</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <Context>
            <Context.Menu icon={IconFont.CogThick}>
              <Context.Item label="Export" action={this.handleExport} />
            </Context.Menu>
            <Context.Menu
              icon={IconFont.Trash}
              color={ComponentColor.Danger}
              testID="context-delete-menu"
            >
              <Context.Item
                label="Delete"
                action={onDeleteVariable}
                value={variable}
                testID="context-delete-task"
              />
            </Context.Menu>
          </Context>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleExport = () => {
    const {
      router,
      variable,
      params: {orgID},
    } = this.props
    router.push(`orgs/${orgID}/variables/${variable.id}/export`)
  }

  private handleUpdateVariableName = async (name: string) => {
    const {onUpdateVariableName, variable} = this.props

    await onUpdateVariableName({id: variable.id, name})
  }

  private handleEditVariable = (): void => {
    this.props.onEditVariable(this.props.variable)
  }
}

export default withRouter<OwnProps>(VariableRow)
