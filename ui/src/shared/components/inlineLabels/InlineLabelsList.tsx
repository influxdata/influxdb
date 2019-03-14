// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {EmptyState} from 'src/clockface'
import {ComponentSize} from '@influxdata/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import InlineLabelsListItem from 'src/shared/components/inlineLabels/InlineLabelsListItem'
import InlineLabelsCreateLabelButton from 'src/shared/components/inlineLabels/InlineLabelsCreateLabelButton'

// Constants
import {ADD_NEW_LABEL_ITEM_ID} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ILabel} from '@influxdata/influx'

interface Props {
  searchTerm: string
  selectedItemID: string
  onUpdateSelectedItemID: (labelID: string) => void
  filteredLabels: ILabel[]
  onItemClick: (labelID: string) => void
  allLabelsUsed: boolean
  onStartCreatingLabel: () => void
}

@ErrorHandling
class InlineLabelsList extends Component<Props> {
  public render() {
    return (
      <div className="inline-labels--list-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="inline-labels--list">{this.menuItems}</div>
        </FancyScrollbar>
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
          size={ComponentSize.Small}
          testID="inline-labels-list--used-all"
        >
          <EmptyState.Text text="This resource has all available labels, LINEBREAK start typing to create a new label" />
        </EmptyState>
      )
    }

    if (!searchTerm) {
      return (
        <EmptyState
          size={ComponentSize.Small}
          testID="inline-labels-list--none-exist"
        >
          <EmptyState.Text text="Start typing to create a new label" />
        </EmptyState>
      )
    }
  }
}

export default InlineLabelsList
