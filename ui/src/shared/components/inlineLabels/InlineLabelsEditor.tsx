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

// Constants
export const ADD_NEW_LABEL_ITEM_ID = 'add-new-label'
export const ADD_NEW_LABEL_LABEL: ILabel = {
  id: ADD_NEW_LABEL_ITEM_ID,
  name: '',
  properties: {
    color: '#000000',
    description: '',
  },
}

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

    const labelsUsed =
      labels.length > 0 && labels.length === selectedLabels.length

    if (isPopoverVisible) {
      return (
        <InlineLabelPopover
          searchTerm={searchTerm}
          selectedItemID={selectedItemID}
          onUpdateSelectedItemID={this.handleUpdateSelectedItemID}
          allLabelsUsed={labelsUsed}
          onDismiss={this.handleDismissPopover}
          onStartCreatingLabel={this.handleStartCreatingLabel}
          onInputChange={this.handleInputChange}
          filteredLabels={this.filterLabels(searchTerm)}
          onAddLabel={this.handleAddLabel}
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
      this.selectAvailableItem()
      onAddLabel(label)
    }
  }

  private selectAvailableItem = (): void => {
    const {searchTerm} = this.state

    const filteredLabels = this.filterLabels(searchTerm)

    if (filteredLabels.length) {
      this.handleUpdateSelectedItemID(filteredLabels[0].id)
    }
  }

  private handleUpdateSelectedItemID = (selectedItemID: string): void => {
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

    const selectedItemID = this.availableLabels[0].id
    this.setState({isPopoverVisible: true, selectedItemID, searchTerm: ''})
  }

  private handleDismissPopover = () => {
    this.setState({isPopoverVisible: false})
  }

  private handleInputChange = async (
    e: ChangeEvent<HTMLInputElement>
  ): Promise<void> => {
    const searchTerm = e.target.value
    const filteredLabels = this.filterLabels(searchTerm)

    if (filteredLabels.length) {
      const selectedItemID = filteredLabels[0].id
      this.setState({searchTerm, selectedItemID})
    } else {
      this.setState({searchTerm})
    }
  }

  private filterLabels = (searchTerm: string): ILabel[] => {
    const filteredLabels = this.availableLabels.filter(label => {
      const lowercaseName = label.name.toLowerCase()
      const lowercaseSearchTerm = searchTerm.toLowerCase()

      return lowercaseName.includes(lowercaseSearchTerm)
    })

    const searchTermHasExactMatch = filteredLabels.reduce(
      (acc: boolean, current: ILabel) => {
        return acc === true || current.name === searchTerm
      },
      false
    )

    if (!searchTermHasExactMatch && searchTerm) {
      return this.filteredLabelsWithAddButton(filteredLabels)
    }

    return this.filteredLabelsWithoutAddButton(filteredLabels)
  }

  private filteredLabelsWithAddButton = (
    filteredLabels: ILabel[]
  ): ILabel[] => {
    const {searchTerm} = this.state

    const updatedAddButton = {...ADD_NEW_LABEL_LABEL, name: searchTerm}

    const addButton = filteredLabels.find(
      label => label.id === updatedAddButton.id
    )

    if (addButton) {
      return filteredLabels.map(fl => {
        return fl.id === updatedAddButton.id ? updatedAddButton : fl
      })
    }

    return [updatedAddButton, ...filteredLabels]
  }

  private filteredLabelsWithoutAddButton = (
    filteredLabels: ILabel[]
  ): ILabel[] => {
    return filteredLabels.filter(label => label.id !== ADD_NEW_LABEL_ITEM_ID)
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
