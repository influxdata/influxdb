// Libraries
import React, {Component, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {
  Button,
  ButtonShape,
  IconFont,
  ComponentColor,
} from '@influxdata/clockface'
import InlineLabelPopover from 'src/shared/components/inlineLabels/InlineLabelPopover'
import CreateLabelOverlay from 'src/configuration/components/CreateLabelOverlay'

// Types
import {ILabel} from '@influxdata/influx'
import {OverlayState} from 'src/types/overlay'

// Utils
import {validateLabelUniqueness} from 'src/configuration/utils/labels'

// Styles
import 'src/shared/components/inlineLabels/InlineLabelsEditor.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selectedLabels: ILabel[]
  labels: ILabel[]
  onAddLabel: (label: ILabel) => void
  onCreateLabel: (label: ILabel) => Promise<ILabel>
}

interface State {
  searchTerm: string
  filteredLabels: ILabel[]
  isPopoverVisible: boolean
  selectedItemID: string
  isCreatingLabel: OverlayState
}

@ErrorHandling
class InlineLabelsEditor extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    const initialFilteredLabels = _.differenceBy(
      props.labels,
      props.selectedLabels,
      label => label.name
    )

    this.state = {
      selectedItemID: null,
      searchTerm: '',
      filteredLabels: initialFilteredLabels,
      isPopoverVisible: false,
      isCreatingLabel: OverlayState.Closed,
    }
  }

  public render() {
    const {isCreatingLabel, searchTerm} = this.state

    return (
      <>
        <div className="inline-labels--editor">
          <div className="inline-labels--add">
            <Button
              color={ComponentColor.Secondary}
              titleText="Add labels"
              onClick={this.handleShowPopover}
              shape={ButtonShape.Square}
              icon={IconFont.Plus}
              testID="inline-labels--add"
            />
          </div>
          {this.popover}
          {this.noLabelsIndicator}
        </div>
        <CreateLabelOverlay
          isVisible={isCreatingLabel === OverlayState.Open}
          onDismiss={this.handleStopCreatingLabel}
          overrideDefaultName={searchTerm}
          onCreateLabel={this.handleCreateLabel}
          onNameValidation={this.handleEnsureUniqueLabelName}
        />
      </>
    )
  }

  private get popover(): JSX.Element {
    const {labels, selectedLabels} = this.props
    const {searchTerm, isPopoverVisible, selectedItemID} = this.state

    const labelsUsed = labels.length === selectedLabels.length

    if (isPopoverVisible) {
      return (
        <InlineLabelPopover
          searchTerm={searchTerm}
          selectedItemID={selectedItemID}
          allLabelsUsed={labelsUsed}
          onDismiss={this.handleDismissPopover}
          onStartCreatingLabel={this.handleStartCreatingLabel}
          onInputChange={this.handleInputChange}
          filteredLabels={this.availableLabels}
          onAddLabel={this.handleAddLabel}
          onUpdateSelectedItem={this.handleUpdateSelectedItem}
        />
      )
    }
  }

  private get noLabelsIndicator(): JSX.Element {
    const {selectedLabels} = this.props

    if (selectedLabels.length) {
      return
    }

    return (
      <div
        className="label label--xs label--colorless"
        onClick={this.handleShowPopover}
        data-testid="inline-labels--empty"
      >
        <span className="label--name">Add a label</span>
      </div>
    )
  }

  private handleAddLabel = (labelID: string): void => {
    const {onAddLabel, labels} = this.props

    const label = labels.find(label => label.id === labelID)

    onAddLabel(label)
  }

  private handleUpdateSelectedItem = (selectedItemID: string): void => {
    this.setState({selectedItemID})
  }

  private handleShowPopover = () => {
    const {availableLabels} = this
    const {isPopoverVisible} = this.state

    if (_.isEmpty(availableLabels) && !isPopoverVisible) {
      return this.setState({
        isPopoverVisible: true,
        selectedItemID: null,
        searchTerm: '',
      })
    }

    const selectedItemID = this.availableLabels[0].name
    this.setState({isPopoverVisible: true, selectedItemID, searchTerm: ''})
  }

  private handleDismissPopover = () => {
    const {labels: filteredLabels} = this.props

    this.setState({isPopoverVisible: false, filteredLabels})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    let selectedItemID = this.state.selectedItemID
    const {labels, selectedLabels} = this.props
    const searchTerm = e.target.value

    const availableLabels = _.differenceBy(labels, selectedLabels, l => l.name)

    const filteredLabels = availableLabels.filter(label => {
      return label.name.includes(searchTerm)
    })

    const selectedItemIDAvailable = filteredLabels.find(
      al => al.name === selectedItemID
    )

    if (!selectedItemIDAvailable && filteredLabels.length) {
      selectedItemID = filteredLabels[0].name
    }

    if (searchTerm.length === 0) {
      return this.setState({
        filteredLabels: this.props.labels,
        selectedItemID: null,
        searchTerm: '',
      })
    }

    this.setState({searchTerm, filteredLabels, selectedItemID})
  }

  private get availableLabels(): ILabel[] {
    const {selectedLabels} = this.props
    const {filteredLabels} = this.state

    return _.differenceBy(filteredLabels, selectedLabels, label => label.name)
  }

  private handleCreateLabel = async (label: ILabel) => {
    const {onCreateLabel, onAddLabel} = this.props
    const newLabel = await onCreateLabel(label)
    onAddLabel(newLabel)
  }

  private handleStartCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Open})
    this.handleDismissPopover()
  }

  private handleStopCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Closed})
  }

  private handleEnsureUniqueLabelName = (name: string): string | null => {
    const {labels} = this.props
    const names = labels.map(label => label.name)

    return validateLabelUniqueness(names, name)
  }
}

export default InlineLabelsEditor
