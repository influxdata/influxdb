// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

// Components
import {Input} from 'src/clockface'
import InlineLabelsList from 'src/shared/components/inlineLabels/InlineLabelsList'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {IconFont} from 'src/clockface/types'
import {ILabel} from '@influxdata/influx'

import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

interface Props {
  searchTerm: string
  selectedItemID: string
  allLabelsUsed: boolean
  onDismiss: () => void
  onStartCreatingLabel: () => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  filteredLabels: ILabel[]
  onAddLabel: (labelID: string) => void
  onUpdateSelectedItem: (highlightedID: string) => void
}

@ErrorHandling
export default class InlineLabelPopover extends Component<Props> {
  public render() {
    const {
      searchTerm,
      allLabelsUsed,
      selectedItemID,
      onAddLabel,
      onDismiss,
      onStartCreatingLabel,
      onUpdateSelectedItem,
      onInputChange,
      filteredLabels,
    } = this.props
    return (
      <ClickOutside onClickOutside={onDismiss}>
        <div
          className="inline-labels--popover"
          data-testid="inline-labels--popover"
        >
          <h5 className="inline-labels--popover-heading">Add Labels</h5>
          <Input
            icon={IconFont.Search}
            placeholder="Filter labels..."
            value={searchTerm}
            onKeyDown={this.handleKeyDown}
            onChange={onInputChange}
            autoFocus={true}
            onBlur={this.handleRefocusInput}
          />
          <InlineLabelsList
            searchTerm={searchTerm}
            allLabelsUsed={allLabelsUsed}
            filteredLabels={filteredLabels}
            selectedItemID={selectedItemID}
            onItemClick={onAddLabel}
            onUpdateSelectedItem={onUpdateSelectedItem}
            onStartCreatingLabel={onStartCreatingLabel}
          />
        </div>
      </ClickOutside>
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    const {selectedItemID, onDismiss, onAddLabel} = this.props

    if (!selectedItemID) {
      return
    }

    switch (e.key) {
      case 'Escape':
        return onDismiss()
      case 'Enter':
        return onAddLabel(selectedItemID)
      case 'ArrowUp':
        return this.handleHighlightAdjacentItem(ArrowDirection.Up)
      case 'ArrowDown':
        return this.handleHighlightAdjacentItem(ArrowDirection.Down)
      default:
        break
    }
  }

  private handleHighlightAdjacentItem = (direction: ArrowDirection): void => {
    const {selectedItemID, filteredLabels, onUpdateSelectedItem} = this.props

    if (!filteredLabels.length || !selectedItemID) {
      return null
    }

    const highlightedIndex = _.findIndex(
      filteredLabels,
      label => label.name === selectedItemID
    )

    const adjacentIndex = Math.min(
      Math.max(highlightedIndex + direction, 0),
      filteredLabels.length - 1
    )

    const adjacentID = filteredLabels[adjacentIndex].id

    onUpdateSelectedItem(adjacentID)
  }

  private handleRefocusInput = (e: ChangeEvent<HTMLInputElement>): void => {
    e.target.focus()
  }
}
