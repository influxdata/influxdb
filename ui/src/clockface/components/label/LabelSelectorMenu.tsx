// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import LabelSelectorMenuItem from 'src/clockface/components/label/LabelSelectorMenuItem'
import ResourceLabelForm from 'src/shared/components/ResourceLabelForm'

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
class LabelSelectorMenu extends Component<Props> {
  public render() {
    return (
      <div className="label-selector--menu-container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={250}>
          <div className="label-selector--menu">
            {this.menuItems}
            {this.emptyText}
            {this.resourceLabelForm}
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
  }

  private get emptyText(): JSX.Element {
    const {allLabelsUsed, filterValue} = this.props

    if (!filterValue) {
      return null
    }

    let text = `No labels match "${filterValue}" want to create a new label?`

    if (allLabelsUsed) {
      text = 'You have somehow managed to add all the labels, wow!'
    }

    return <div className="label-selector--empty">{text}</div>
  }

  private get resourceLabelForm(): JSX.Element {
    const {filterValue, onCreateLabel, filteredLabels} = this.props

    if (!filterValue || filteredLabels.find(l => l.name === filterValue)) {
      return
    }

    return (
      <ResourceLabelForm labelName={filterValue} onSubmit={onCreateLabel} />
    )
  }
}

export default LabelSelectorMenu
