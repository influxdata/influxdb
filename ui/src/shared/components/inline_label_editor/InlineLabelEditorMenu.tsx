// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
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
  onCreateLabel: (label: Label) => Promise<void>
}

@ErrorHandling
class InlineLabelEditorMenu extends Component<Props> {
  public render() {
    return (
      <div className="inline-label-editor--menu-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="inline-label-editor--menu">
            {this.menuItems}
            {this.createNewLabelButton}
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
  }

  private get createNewLabelButton(): JSX.Element {
    const {filterValue, filteredLabels} = this.props

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

    return <div>{`Create new label "${filterValue}"`}</div>
  }
}

export default InlineLabelEditorMenu
