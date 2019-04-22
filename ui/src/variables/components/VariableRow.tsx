// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps, Link} from 'react-router'

// Components
import {IndexList, Alignment, Context, IconFont} from 'src/clockface'
import {
  ComponentColor,
  ComponentSpacer,
  FlexDirection,
  AlignItems,
  ComponentSize,
  Button,
} from '@influxdata/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Types
import {IVariable as Variable, ILabel} from '@influxdata/influx'
import {AppState} from 'src/types'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Actions
import {
  addVariableLabelsAsync,
  removeVariableLabelsAsync,
} from 'src/variables/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'

interface OwnProps {
  variable: Variable
  onDeleteVariable: (variable: Variable) => void
  onUpdateVariableName: (variable: Partial<Variable>) => void
  onEditVariable: (variable: Variable) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
}

interface DispatchProps {
  onAddVariableLabels: typeof addVariableLabelsAsync
  onRemoveVariableLabels: typeof removeVariableLabelsAsync
  onCreateLabel: typeof createLabelAsync
}

type Props = OwnProps & DispatchProps & StateProps

class VariableRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {variable, onDeleteVariable} = this.props

    return (
      <IndexList.Row testID="variable-row">
        <IndexList.Cell alignment={Alignment.Left}>
          <ComponentSpacer
            margin={ComponentSize.Small}
            direction={FlexDirection.Column}
            alignItems={AlignItems.FlexStart}
            stretchToFitWidth={true}
          >
            <div className="editable-name">
              <Link to={this.editVariablePath}>
                <span>{variable.name}</span>
              </Link>
            </div>
            {this.labels}
          </ComponentSpacer>
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Left}>Query</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <Button
            text="Rename"
            onClick={this.handleRenameVariable}
            color={ComponentColor.Danger}
            size={ComponentSize.ExtraSmall}
          />
        </IndexList.Cell>
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

  private get editVariablePath(): string {
    const {
      variable,
      params: {orgID},
    } = this.props

    return `/orgs/${orgID}/variables/${variable.id}/edit`
  }

  private get labels(): JSX.Element {
    const {variable, labels, onFilterChange} = this.props
    const collectorLabels = viewableLabels(variable.labels)

    return (
      <InlineLabels
        selectedLabels={collectorLabels}
        labels={labels}
        onFilterChange={onFilterChange}
        onAddLabel={this.handleAddLabel}
        onRemoveLabel={this.handleRemoveLabel}
        onCreateLabel={this.handleCreateLabel}
      />
    )
  }

  private handleAddLabel = (label: ILabel): void => {
    const {variable, onAddVariableLabels} = this.props

    onAddVariableLabels(variable.id, [label])
  }

  private handleRemoveLabel = (label: ILabel): void => {
    const {variable, onRemoveVariableLabels} = this.props

    onRemoveVariableLabels(variable.id, [label])
  }

  private handleCreateLabel = async (label: ILabel): Promise<void> => {
    try {
      const {name, properties} = label
      await this.props.onCreateLabel(name, properties)
    } catch (err) {
      throw err
    }
  }

  private handleExport = () => {
    const {
      router,
      variable,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/variables/${variable.id}/export`)
  }

  private handleRenameVariable = async () => {
    const {
      router,
      variable,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/variables/${variable.id}/rename`)
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list),
  }
}

const mdtp: DispatchProps = {
  onCreateLabel: createLabelAsync,
  onAddVariableLabels: addVariableLabelsAsync,
  onRemoveVariableLabels: removeVariableLabelsAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(VariableRow))
