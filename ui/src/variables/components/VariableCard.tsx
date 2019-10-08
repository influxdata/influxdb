// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceCard} from '@influxdata/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import VariableContextMenu from 'src/variables/components/VariableContextMenu'
import OverlayLink from 'src/overlays/components/OverlayLink'

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

class VariableCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {variable, onDeleteVariable} = this.props

    return (
      <ResourceCard
        testID="resource-card"
        labels={this.labels}
        contextMenu={
          <VariableContextMenu
            variable={variable}
            onDelete={onDeleteVariable}
          />
        }
        name={
          <OverlayLink overlayID="edit-variable" resourceID={variable.id}>
            {onClick => (
              <ResourceCard.Name onClick={onClick} name={variable.name} />
            )}
          </OverlayLink>
        }
        metaData={[<>Type: {variable.arguments.type}</>]}
      />
    )
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
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list as ILabel[]),
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
)(withRouter<Props>(VariableCard))
