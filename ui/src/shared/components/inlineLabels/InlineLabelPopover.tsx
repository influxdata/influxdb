// Libraries
import React, {
  PureComponent,
  ChangeEvent,
  KeyboardEvent,
  RefObject,
} from 'react'
import _ from 'lodash'

// Components
import {Input, ButtonBaseRef} from '@influxdata/clockface'
import InlineLabelsList from 'src/shared/components/inlineLabels/InlineLabelsList'

// Constants
import {ADD_NEW_LABEL_ITEM_ID} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Types
import {Label} from 'src/types'
import {
  Appearance,
  IconFont,
  Popover,
  PopoverPosition,
  PopoverInteraction,
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

interface Props {
  searchTerm: string
  triggerRef: RefObject<ButtonBaseRef>
  selectedItemID: string
  onUpdateSelectedItemID: (highlightedID: string) => void
  allLabelsUsed: boolean
  onStartCreatingLabel: () => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  filteredLabels: Label[]
  onAddLabel: (labelID: string) => void
}

@ErrorHandling
export default class InlineLabelPopover extends PureComponent<Props> {
  public render() {
    const {
      searchTerm,
      allLabelsUsed,
      selectedItemID,
      onAddLabel,
      triggerRef,
      onStartCreatingLabel,
      onUpdateSelectedItemID,
      onInputChange,
      filteredLabels,
    } = this.props

    return (
      <Popover
        appearance={Appearance.Outline}
        position={PopoverPosition.Below}
        triggerRef={triggerRef}
        distanceFromTrigger={8}
        showEvent={PopoverInteraction.Click}
        hideEvent={PopoverInteraction.Click}
        testID="inline-labels--popover"
        className="inline-labels--popover"
        contents={() => (
          <>
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
              maxLength={30}
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
          </>
        )}
      />
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    const {selectedItemID, onAddLabel, onStartCreatingLabel} = this.props

    switch (e.key) {
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
