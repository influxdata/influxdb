// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

// Components
import {Input} from '@influxdata/clockface'
import InlineLabelsList from 'src/shared/components/inlineLabels/InlineLabelsList'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Constants
import {ADD_NEW_LABEL_ITEM_ID} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Types
import {ILabel} from '@influxdata/influx'
import {IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

interface Props {
  searchTerm: string
  selectedItemID: string
  onUpdateSelectedItemID: (highlightedID: string) => void
  allLabelsUsed: boolean
  onDismiss: () => void
  onStartCreatingLabel: () => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  filteredLabels: ILabel[]
  onAddLabel: (labelID: string) => void
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
      onUpdateSelectedItemID,
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
            testID="inline-labels--popover-field"
          />
          <InlineLabelsList
            searchTerm={searchTerm}
            allLabelsUsed={allLabelsUsed}
            filteredLabels={filteredLabels}
            selectedItemID={selectedItemID}
            onItemClick={onAddLabel}
            onUpdateSelectedItemID={onUpdateSelectedItemID}
            onStartCreatingLabel={onStartCreatingLabel}
          />
        </div>
      </ClickOutside>
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    const {
      selectedItemID,
      onDismiss,
      onAddLabel,
      onStartCreatingLabel,
    } = this.props

    switch (e.key) {
      case 'Escape':
        onDismiss()
        break
      case 'Enter':
        e.preventDefault()
        e.stopPropagation()
        if (selectedItemID === ADD_NEW_LABEL_ITEM_ID) {
          onStartCreatingLabel()
          break
        }

        if (selectedItemID) {
          onAddLabel(selectedItemID)
          break
        }
      case 'ArrowUp':
        this.handleHighlightAdjacentItem(ArrowDirection.Up)
        break
      case 'ArrowDown':
        this.handleHighlightAdjacentItem(ArrowDirection.Down)
        break
      default:
        break
    }
  }

  private handleHighlightAdjacentItem = (direction: ArrowDirection): void => {
    const {selectedItemID, filteredLabels, onUpdateSelectedItemID} = this.props

    if (!filteredLabels.length || !selectedItemID) {
      return null
    }

    const selectedItemIndex = _.findIndex(
      filteredLabels,
      label => label.id === selectedItemID
    )

    const adjacentIndex = Math.min(
      Math.max(selectedItemIndex + direction, 0),
      filteredLabels.length - 1
    )

    const adjacentID = filteredLabels[adjacentIndex].id

    onUpdateSelectedItemID(adjacentID)
  }

  private handleRefocusInput = (e: ChangeEvent<HTMLInputElement>): void => {
    e.target.focus()
  }
}
