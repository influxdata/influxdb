// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {Label as LabelComponent} from '@influxdata/clockface'
import InlineLabelsEditor from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Types
import {Label} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

export enum LabelsEditMode {
  Editable = 'editable',
  Readonly = 'readonly',
}

interface Props {
  editMode?: LabelsEditMode // temporary for displaying labels
  selectedLabels: Label[]
  labels: Label[]
  onRemoveLabel?: (label: Label) => void
  onAddLabel?: (label: Label) => void
  onCreateLabel?: (label: Label) => void
  onFilterChange?: (searchTerm: string) => void
}

@ErrorHandling
export default class InlineLabels extends Component<Props> {
  public static defaultProps = {
    editMode: LabelsEditMode.Editable,
  }

  public render() {
    return <div className="inline-labels">{this.selectedLabels}</div>
  }

  private get selectedLabels(): JSX.Element {
    const {selectedLabels, labels, onAddLabel, onCreateLabel} = this.props

    return (
      <div className="inline-labels--container">
        {this.isEditable && (
          <InlineLabelsEditor
            labels={labels}
            selectedLabels={selectedLabels}
            onAddLabel={onAddLabel}
            onCreateLabel={onCreateLabel}
          />
        )}
        {this.currentLabels}
      </div>
    )
  }

  private get currentLabels(): JSX.Element[] {
    const {selectedLabels} = this.props
    const onDelete = this.isEditable ? this.handleDeleteLabel : null

    if (selectedLabels.length) {
      return selectedLabels.map(label => (
        <LabelComponent
          id={label.id}
          key={label.id}
          name={label.name}
          color={label.properties.color}
          description={label.properties.description}
          onDelete={onDelete}
          onClick={this.handleLabelClick}
        />
      ))
    }
  }

  private get isEditable(): boolean {
    return this.props.editMode === LabelsEditMode.Editable
  }

  private handleLabelClick = (labelID: string): void => {
    const {onFilterChange, labels} = this.props

    const labelName = labels.find(l => l.id === labelID).name

    onFilterChange(labelName)
  }

  private handleDeleteLabel = (labelID: string): void => {
    const {onRemoveLabel, selectedLabels} = this.props
    const label = selectedLabels.find(label => label.id === labelID)

    onRemoveLabel(label)
  }
}
