// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import LabelSelectorMenuItem from 'src/clockface/components/label/LabelSelectorMenuItem'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Label} from 'src/api'

interface Props {
  highlightItemID: string
  filteredLabels: Label[]
  onItemClick: (labelID: string) => void
  onItemHighlight: (labelID: string) => void
  allLabelsUsed: boolean
}

@ErrorHandling
class LabelSelectorMenu extends Component<Props> {
  public render() {
    return (
      <div className="label-selector--menu-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="label-selector--menu">{this.menuItems}</div>
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
        <LabelSelectorMenuItem
          highlighted={highlightItemID === label.name}
          key={label.name}
          name={label.name}
          id={label.name}
          description={label.properties.description}
          colorHex={label.properties.color}
          onClick={onItemClick}
          onHighlight={onItemHighlight}
        />
      ))
    }

    return <div className="label-selector--empty">{this.emptyText}</div>
  }

  private get emptyText(): string {
    const {allLabelsUsed} = this.props

    if (allLabelsUsed) {
      return 'You have somehow managed to add all the labels, wow!'
    }

    return 'No labels match your query'
  }
}

export default LabelSelectorMenu
