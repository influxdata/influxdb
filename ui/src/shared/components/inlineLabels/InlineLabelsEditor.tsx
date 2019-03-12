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

// Utils
import {validateLabelUniqueness} from 'src/configuration/utils/labels'

// Types
import {ILabel} from '@influxdata/influx'
import {OverlayState} from 'src/types/overlay'

// Styles
import 'src/shared/components/inlineLabels/InlineLabelsEditor.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selectedLabels: ILabel[]
  labels: ILabel[]
  onAddLabel: (label: ILabel) => void
  onCreateLabel: (label: ILabel) => Promise<void>
}

interface State {
  searchTerm: string
  isPopoverVisible: boolean
  selectedItemID: string
  isCreatingLabel: OverlayState
}

@ErrorHandling
class InlineLabelsEditor extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      selectedItemID: null,
      searchTerm: '',
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
          filteredLabels={this.filteredLabels}
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

    if (label) {
      onAddLabel(label)
    }
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
    this.setState({isPopoverVisible: false})
  }

  private handleInputChange = async (
    e: ChangeEvent<HTMLInputElement>
  ): Promise<void> => {
    const {availableLabels} = this
    let selectedItemID = this.state.selectedItemID
    const searchTerm = e.target.value

    const selectedItemIDAvailable = availableLabels.find(
      al => al.name === selectedItemID
    )

    const matchingLabels = availableLabels.filter(label => {
      return label.name.includes(searchTerm)
    })

    if (!selectedItemIDAvailable && matchingLabels.length) {
      selectedItemID = matchingLabels[0].name
    }

    if (searchTerm.length === 0) {
      selectedItemID = null
    }

    this.setState({searchTerm, selectedItemID})
  }

  private get filteredLabels(): ILabel[] {
    const {searchTerm} = this.state

    return this.availableLabels.filter(label => {
      return label.name.includes(searchTerm)
    })
  }

  private get availableLabels(): ILabel[] {
    const {selectedLabels, labels} = this.props

    return _.differenceBy(labels, selectedLabels, label => label.name)
  }

  private handleCreateLabel = async (label: ILabel) => {
    const {onCreateLabel, onAddLabel} = this.props

    try {
      await onCreateLabel(label)
      const newLabel = this.props.labels.find(l => l.name === label.name)
      await onAddLabel(newLabel)
    } catch (error) {
      console.error(error)
    }
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
