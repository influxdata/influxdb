// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {EmptyState} from 'src/clockface'
import {ComponentSize} from '@influxdata/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import InlineLabelEditorMenuItem from 'src/shared/components/inline_label_editor/InlineLabelEditorMenuItem'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Label} from 'src/types/v2/labels'

interface Props {
  filterValue: string
  highlightItemID: string
  filteredLabels: Label[]
  onItemClick: (labelID: string) => void
  onItemHighlight: (labelID: string) => void
  allLabelsUsed: boolean
  onStartCreatingLabel: () => void
}

@ErrorHandling
class InlineLabelEditorMenu extends Component<Props> {
  public render() {
    return (
      <div className="inline-label-editor--menu-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="inline-label-editor--menu">
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
      onItemHighlight,
      highlightItemID,
      allLabelsUsed,
    } = this.props

    if (filteredLabels.length) {
      return filteredLabels.map(label => (
        <InlineLabelEditorMenuItem
          highlighted={highlightItemID === label.id}
          key={label.id}
          name={label.name}
          id={label.id}
          description={label.properties.description}
          colorHex={label.properties.color}
          onClick={onItemClick}
          onHighlight={onItemHighlight}
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
  }

  private get createNewLabelButton(): JSX.Element {
    const {filterValue, filteredLabels, onStartCreatingLabel} = this.props

    if (!filterValue) {
      return null
    }

    const filterValueHasExactMatch = filteredLabels.reduce(
      (acc: boolean, current: Label) => {
        if (acc === true || current.name === filterValue) {
          return true
        }

        return false
      },
      false
    )

    if (filterValueHasExactMatch) {
      return null
    }

    return (
      <div
        className="inline-label-editor--menu-item inline-label-editor--create-new"
        onClick={onStartCreatingLabel}
      >
        Create new label "<strong>{`${filterValue}`}</strong>"
      </div>
    )
  }
}

export default InlineLabelEditorMenu
