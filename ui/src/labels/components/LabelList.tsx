// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, Overlay} from 'src/clockface'
import UpdateLabelOverlay from 'src/labels/components/UpdateLabelOverlay'
import LabelRow from 'src/labels/components/LabelRow'

// Utils
import {validateLabelUniqueness} from 'src/labels/utils/'

// Types
import {ILabel} from '@influxdata/influx'
import {OverlayState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  labels: ILabel[]
  emptyState: JSX.Element
  onUpdateLabel: (label: ILabel) => void
  onDeleteLabel: (labelID: string) => void
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
        <Overlay visible={this.isOverlayVisible}>
          <UpdateLabelOverlay
            label={this.label}
            onDismiss={this.handleCloseModal}
            onUpdateLabel={this.handleUpdateLabel}
            onNameValidation={this.handleNameValidation}
          />
        </Overlay>
      </>
    )
  }

  private get rows(): JSX.Element[] {
    const {onDeleteLabel} = this.props

    return this.props.labels.map((label, index) => (
      <LabelRow
        key={label.id || `label-${index}`}
        onDelete={onDeleteLabel}
        onClick={this.handleStartEdit}
        label={label}
      />
    ))
  }

  private get label(): ILabel | null {
    if (this.state.labelID) {
      return this.props.labels.find(l => l.id === this.state.labelID)
    }
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

  private handleUpdateLabel = async (updatedLabel: ILabel) => {
    await this.props.onUpdateLabel(updatedLabel)
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleNameValidation = (name: string): string | null => {
    const {labels} = this.props

    const names = labels.map(label => label.name).filter(l => l !== name)

    return validateLabelUniqueness(names, name)
  }
}
