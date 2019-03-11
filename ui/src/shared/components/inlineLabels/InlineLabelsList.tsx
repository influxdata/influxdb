// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {EmptyState} from 'src/clockface'
import {ComponentSize} from '@influxdata/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import InlineLabelsListItem from 'src/shared/components/inlineLabels/InlineLabelsListItem'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ILabel} from '@influxdata/influx'

interface Props {
  searchTerm: string
  selectedItemID: string
  filteredLabels: ILabel[]
  onItemClick: (labelID: string) => void
  onUpdateSelectedItem: (labelID: string) => void
  allLabelsUsed: boolean
  onStartCreatingLabel: () => void
}

@ErrorHandling
class InlineLabelsList extends Component<Props> {
  public render() {
    return (
      <div className="inline-labels--list-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="inline-labels--list">
            {this.createNewLabelButton}
            {this.menuItems}
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get menuItems(): JSX.Element[] | JSX.Element {
    const {
      filteredLabels,
      onItemClick,
      onUpdateSelectedItem,
      selectedItemID,
      allLabelsUsed,
      searchTerm,
    } = this.props

    if (filteredLabels.length) {
      return filteredLabels.map(label => (
        <InlineLabelsListItem
          active={selectedItemID === label.id}
          key={label.id}
          name={label.name}
          id={label.id}
          description={label.properties.description}
          colorHex={label.properties.color}
          onClick={onItemClick}
          onMouseOver={onUpdateSelectedItem}
        />
      ))
    }

    if (allLabelsUsed) {
      return (
        <EmptyState size={ComponentSize.Small}>
          <EmptyState.Text text="This resource uses all available labels" />
        </EmptyState>
      )
    }

    if (!searchTerm) {
      return (
        <EmptyState size={ComponentSize.Small}>
          <EmptyState.Text text="Type to create your first label" />
        </EmptyState>
      )
    }
  }

  private get createNewLabelButton(): JSX.Element {
    const {searchTerm, filteredLabels, onStartCreatingLabel} = this.props

    if (!searchTerm) {
      return null
    }

    const searchTermHasExactMatch = filteredLabels.reduce(
      (acc: boolean, current: ILabel) => {
        return acc === true || current.name === searchTerm
      },
      false
    )

    if (searchTermHasExactMatch) {
      return null
    }

    return (
      <div
        className="inline-labels--list-item inline-labels--create-new"
        onClick={onStartCreatingLabel}
        data-testid="inline-labels--create-new"
      >
        Create new label "<strong>{`${searchTerm}`}</strong>"
      </div>
    )
  }
}

export default InlineLabelsList
