// Libraries
import React, {PureComponent} from 'react'

// Components
import {Overlay, ResourceList} from '@influxdata/clockface'
import UpdateLabelOverlay from 'src/labels/components/UpdateLabelOverlay'
import LabelCard from 'src/labels/components/LabelCard'

// Utils
import {validateLabelUniqueness} from 'src/labels/utils/'
import memoizeOne from 'memoize-one'

// Types
import {OverlayState, Label} from 'src/types'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

interface Props {
  labels: Label[]
  emptyState: JSX.Element
  onUpdateLabel: (label: Label) => void
  onDeleteLabel: (labelID: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
}

interface State {
  labelID: string
  overlayState: OverlayState
}

@ErrorHandling
export default class LabelList extends PureComponent<Props, State> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public state: State = {
    labelID: null,
    overlayState: OverlayState.Closed,
  }

  public render() {
    return (
      <>
        <ResourceList>
          <ResourceList.Body emptyState={this.props.emptyState}>
            {this.rows}
          </ResourceList.Body>
        </ResourceList>
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
    const {labels, sortKey, sortDirection, sortType, onDeleteLabel} = this.props
    const sortedLabels = this.memGetSortedResources(
      labels,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedLabels.map((label, index) => (
      <LabelCard
        key={label.id || `label-${index}`}
        onDelete={onDeleteLabel}
        onClick={this.handleStartEdit}
        label={label}
      />
    ))
  }

  private get label(): Label | null {
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

  private handleUpdateLabel = (updatedLabel: Label) => {
    this.props.onUpdateLabel(updatedLabel)
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleNameValidation = (name: string): string | null => {
    const {labels} = this.props

    const names = labels.map(label => label.name).filter(l => l !== name)

    return validateLabelUniqueness(names, name)
  }
}
