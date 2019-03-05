// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {Label} from 'src/clockface'
import InlineLabelsEditor from 'src/shared/components/inlineLabels/InlineLabelsEditor'
import CreateLabelOverlay from 'src/configuration/components/CreateLabelOverlay'

// Types
import {Label as LabelType} from '@influxdata/influx'
import {OverlayState} from 'src/types/overlay'

// Utils
import {validateLabelUniqueness} from 'src/configuration/utils/labels'

// Styles
import 'src/shared/components/inlineLabels/InlineLabels.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selectedLabels: LabelType[]
  labels: LabelType[]
  onRemoveLabel: (label: LabelType) => void
  onAddLabel: (label: LabelType) => void
  onCreateLabel: (label: LabelType) => void
  onFilterChange: (searchTerm: string) => void
}

interface State {
  isCreatingLabel: OverlayState
  overrideLabelName: string | null
}

@ErrorHandling
export default class InlineLabels extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isCreatingLabel: OverlayState.Closed,
      overrideLabelName: null,
    }
  }

  public render() {
    return <div className="inline-labels">{this.selectedLabels}</div>
  }

  private get selectedLabels(): JSX.Element {
    const {isCreatingLabel, overrideLabelName} = this.state
    const {selectedLabels, labels, onAddLabel, onCreateLabel} = this.props

    return (
      <>
        <div className="inline-labels--container">
          <InlineLabelsEditor
            labels={labels}
            selectedLabels={selectedLabels}
            onAddLabel={onAddLabel}
            onCreateLabel={this.handleStartCreatingLabel}
          />
          {this.currentLabels}
        </div>
        <CreateLabelOverlay
          isVisible={isCreatingLabel === OverlayState.Open}
          onDismiss={this.handleStopCreatingLabel}
          onCreateLabel={onCreateLabel}
          onNameValidation={this.handleNameValidation}
          overrideDefaultName={overrideLabelName}
        />
      </>
    )
  }

  private get currentLabels(): JSX.Element[] {
    const {selectedLabels} = this.props

    if (selectedLabels.length) {
      return selectedLabels.map(label => (
        <Label
          id={label.id}
          key={label.id}
          name={label.name}
          colorHex={label.properties.color}
          description={label.properties.description}
          onDelete={this.handleDeleteLabel}
          onClick={this.handleLabelClick}
        />
      ))
    }
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

  private handleStopCreatingLabel = (): void => {
    this.setState({
      isCreatingLabel: OverlayState.Closed,
      overrideLabelName: null,
    })
  }

  private handleStartCreatingLabel = (name: string): void => {
    this.setState({overrideLabelName: name, isCreatingLabel: OverlayState.Open})
  }

  private handleNameValidation = (name: string): string | null => {
    const names = this.props.labels.map(label => label.name)

    return validateLabelUniqueness(names, name)
  }
}
