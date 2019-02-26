// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {Label} from 'src/clockface'

// Types
import {Label as LabelType} from 'src/types/v2/labels'

// Styles
import 'src/shared/components/inline_label_editor/InlineLabelEditor.scss'

interface Props {
  selectedLabels: LabelType[]
  labels: LabelType[]
  onRemoveLabel: (label: LabelType) => void
}

export default class InlineLabelEditor extends Component<Props> {
  public render() {
    return <div className="inline-label-editor">{this.selectedLabels}</div>
  }

  private get selectedLabels(): JSX.Element {
    const {selectedLabels} = this.props

    if (selectedLabels.length) {
      return (
        <div className="inline-label-editor--margin">
          {selectedLabels.map(label => (
            <Label
              id={label.id}
              key={label.id}
              name={label.name}
              colorHex={label.properties.color}
              description={label.properties.description}
              onDelete={this.handleDeleteLabel}
            />
          ))}
        </div>
      )
    }

    return <div className="inline-label-editor--empty">No labels</div>
  }

  private handleDeleteLabel = (labelID: string): void => {
    const {onRemoveLabel, selectedLabels} = this.props
    const label = selectedLabels.find(label => label.id === labelID)

    onRemoveLabel(label)
  }
}
