// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, OverlayTechnology} from 'src/clockface'
import UpdateLabelOverlay from 'src/configuration/components/UpdateLabelOverlay'
import LabelRow from 'src/configuration/components/LabelRow'

// Utils
import {validateLabelUniqueness} from 'src/configuration/utils/labels'

// Types
import {LabelType} from 'src/clockface'
import {OverlayState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  labels: LabelType[]
  emptyState: JSX.Element
  onUpdateLabel: (label: LabelType) => void
}

interface State {
  labelID: string
  overlayState: OverlayState
}

@ErrorHandling
export default class LabelList extends PureComponent<Props, State> {
  public state: State = {
    labelID: null,
    overlayState: OverlayState.Closed,
  }

  public render() {
    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="20%" />
            <IndexList.HeaderCell columnName="Description" width="55%" />
            <IndexList.HeaderCell width="25%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
            {this.rows}
          </IndexList.Body>
        </IndexList>
        <OverlayTechnology visible={this.isOverlayVisible}>
          <UpdateLabelOverlay
            label={this.label}
            onDismiss={this.handleCloseModal}
            onUpdateLabel={this.handleUpdateLabel}
            onNameValidation={this.handleNameValidation}
          />
        </OverlayTechnology>
      </>
    )
  }

  private get rows(): JSX.Element[] {
    return this.props.labels.map((label, index) => (
      <LabelRow
        key={label.id || `label-${index}`}
        label={{
          ...label,
          onClick: this.handleStartEdit,
        }}
      />
    ))
  }

  private get label(): LabelType {
    return this.props.labels.find(b => b.id === this.state.labelID)
  }

  private handleCloseModal = () => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleStartEdit = (labelID: string): void => {
    this.setState({labelID, overlayState: OverlayState.Open})
  }

  private get isOverlayVisible(): boolean {
    const {labelID, overlayState} = this.state
    return !!labelID && overlayState === OverlayState.Open
  }

  private handleUpdateLabel = async (updatedLabel: LabelType) => {
    await this.props.onUpdateLabel(updatedLabel)
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleNameValidation = (name: string): string | null => {
    const {labels} = this.props

    const names = labels.map(label => label.name)

    return validateLabelUniqueness(names, name)
  }
}
