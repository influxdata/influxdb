// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {EmptyState, DapperScrollbars} from '@influxdata/clockface'
import InlineLabelsListItem from 'src/shared/components/inlineLabels/InlineLabelsListItem'
import InlineLabelsCreateLabelButton from 'src/shared/components/inlineLabels/InlineLabelsCreateLabelButton'

// Constants
import {ADD_NEW_LABEL_ITEM_ID} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Label} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
  selectedItemID: string
  onUpdateSelectedItemID: (labelID: string) => void
  filteredLabels: Label[]
  onItemClick: (labelID: string) => void
  allLabelsUsed: boolean
  onStartCreatingLabel: () => void
}

@ErrorHandling
class InlineLabelsList extends Component<Props> {
  public render() {
    return (
      <div className="inline-labels--list-container">
        <DapperScrollbars
          autoSize={true}
          autoHide={false}
          style={{maxWidth: '100%', maxHeight: '300px'}}
          noScrollX={true}
        >
          <div
            className="inline-labels--list"
            data-testid="inline-labels--list"
          >
            {this.menuItems}
          </div>
        </DapperScrollbars>
      </div>
    )
  }

  private get menuItems(): JSX.Element[] | JSX.Element {
    const {
      filteredLabels,
      onItemClick,
      onUpdateSelectedItemID,
      selectedItemID,
      allLabelsUsed,
      searchTerm,
      onStartCreatingLabel,
    } = this.props

    if (filteredLabels.length) {
      return filteredLabels.map(label => {
        if (label.id === ADD_NEW_LABEL_ITEM_ID) {
          return (
            <InlineLabelsCreateLabelButton
              active={selectedItemID === label.id}
              key={label.id}
              name={label.name}
              id={label.id}
              onClick={onStartCreatingLabel}
              onMouseOver={onUpdateSelectedItemID}
            />
          )
        }

        return (
          <InlineLabelsListItem
            active={selectedItemID === label.id}
            key={label.id}
            name={label.name}
            id={label.id}
            description={label.properties.description}
            colorHex={label.properties.color}
            onClick={onItemClick}
            onMouseOver={onUpdateSelectedItemID}
          />
        )
      })
    }

    if (allLabelsUsed) {
      return (
        <EmptyState
          size={ComponentSize.ExtraSmall}
          testID="inline-labels-list--used-all"
        >
          <EmptyState.Text>
            This resource has all available labels,
            <br />
            start typing to create a new label
          </EmptyState.Text>
        </EmptyState>
      )
    }

    if (!searchTerm) {
      return (
        <EmptyState
          size={ComponentSize.ExtraSmall}
          testID="inline-labels-list--none-exist"
        >
          <EmptyState.Text>Start typing to create a new label</EmptyState.Text>
        </EmptyState>
      )
    }
  }
}

export default InlineLabelsList
