// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Label as LabelComponent} from '@influxdata/clockface'
import InlineLabelsEditor from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Types
import {Label, ResourceType, AppState} from 'src/types'

// Selectors
import {getAll, getLabels} from 'src/resources/selectors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

export enum LabelsEditMode {
  Editable = 'editable',
  Readonly = 'readonly',
}

interface OwnProps {
  editMode?: LabelsEditMode // temporary for displaying labels
  selectedLabelIDs: string[]
  onRemoveLabel?: (label: Label) => void
  onAddLabel?: (label: Label) => void
  onFilterChange?: (searchTerm: string) => void
}

interface StateProps {
  labels: Label[]
  selectedLabels: Label[]
}

type Props = StateProps & OwnProps

@ErrorHandling
class InlineLabels extends Component<Props> {
  public static defaultProps = {
    editMode: LabelsEditMode.Editable,
  }

  public render() {
    return <div className="inline-labels">{this.selectedLabels}</div>
  }

  private get selectedLabels(): JSX.Element {
    const {selectedLabels, labels, onAddLabel} = this.props

    return (
      <div className="inline-labels--container">
        {this.isEditable && (
          <InlineLabelsEditor
            labels={labels}
            selectedLabels={selectedLabels}
            onAddLabel={onAddLabel}
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
      return selectedLabels.map(_label => {
        const label = {..._label}

        // TODO: clean this up during normalization effort at
        // the service layer and not in the view (alex)
        if (!label.hasOwnProperty('properties')) {
          label.properties = {
            color: '#FF0000',
            description: '',
          }
        }

        return (
          <LabelComponent
            id={label.id}
            key={label.id}
            name={label.name}
            color={label.properties.color}
            description={label.properties.description}
            onDelete={onDelete}
            onClick={this.handleLabelClick.bind(this, label.name)}
          />
        )
      })
    }
  }

  private get isEditable(): boolean {
    return this.props.editMode === LabelsEditMode.Editable
  }

  private handleLabelClick = (labelName: string) => {
    const {onFilterChange} = this.props

    onFilterChange(labelName)
  }

  private handleDeleteLabel = (labelID: string) => {
    const {onRemoveLabel, selectedLabels} = this.props
    const label = selectedLabels.find(label => label.id === labelID)

    onRemoveLabel(label)
  }
}

const mstp = (state: AppState, props: OwnProps) => {
  const labels = getAll<Label>(state, ResourceType.Labels)
  const selectedLabels = getLabels(state, props.selectedLabelIDs)

  return {labels, selectedLabels}
}

export default connect<StateProps>(mstp)(InlineLabels)
