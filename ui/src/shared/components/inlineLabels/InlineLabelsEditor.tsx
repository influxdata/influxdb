// Libraries
import React, {Component, ChangeEvent, createRef} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import _ from 'lodash'

// Components
import {
  ButtonBase,
  ButtonShape,
  ButtonBaseRef,
  ComponentColor,
  ClickOutside,
} from '@influxdata/clockface'
import InlineLabelPopover from 'src/shared/components/inlineLabels/InlineLabelPopover'
import CreateLabelOverlay from 'src/labels/components/CreateLabelOverlay'

// Utils
import {validateLabelUniqueness} from 'src/labels/utils'

// Types
import {Label, RemoteDataState} from 'src/types'
import {OverlayState} from 'src/types/overlay'
import {createLabel} from 'src/labels/actions/thunks'

// Constants
export const ADD_NEW_LABEL_ITEM_ID = 'add-new-label'
export const ADD_NEW_LABEL_LABEL: Label = {
  id: ADD_NEW_LABEL_ITEM_ID,
  name: '',
  properties: {
    color: '#000000',
    description: '',
  },
  status: RemoteDataState.NotStarted,
}

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  selectedLabels: Label[]
  labels: Label[]
  onAddLabel: (label: Label) => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

interface State {
  searchTerm: string
  selectedItemID: string
  isCreatingLabel: OverlayState
  isPopoverVisible: boolean
}

@ErrorHandling
class InlineLabelsEditor extends Component<Props, State> {
  private popoverTrigger = createRef<ButtonBaseRef>()

  constructor(props: Props) {
    super(props)

    this.state = {
      selectedItemID: null,
      searchTerm: '',
      isCreatingLabel: OverlayState.Closed,
      isPopoverVisible: false,
    }
  }

  public componentDidMount() {
    this.handleAddPopoverEventListener()
  }

  public render() {
    const {isCreatingLabel, searchTerm} = this.state

    return (
      <>
        {this.popover}
        <div className="inline-labels--editor">
          <ButtonBase
            color={ComponentColor.Secondary}
            titleText="Add labels"
            shape={ButtonShape.Square}
            className="inline-labels--add"
            testID="inline-labels--add"
            ref={this.popoverTrigger}
          >
            <div className="inline-labels--add-icon" />
          </ButtonBase>
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
    const {searchTerm, selectedItemID, isPopoverVisible} = this.state

    const labelsUsed =
      labels.length > 0 && labels.length === selectedLabels.length

    return (
      <ClickOutside onClickOutside={this.onClickOutside}>
        <InlineLabelPopover
          searchTerm={searchTerm}
          triggerRef={this.popoverTrigger}
          selectedItemID={selectedItemID}
          onUpdateSelectedItemID={this.handleUpdateSelectedItemID}
          allLabelsUsed={labelsUsed}
          onStartCreatingLabel={this.handleStartCreatingLabel}
          onInputChange={this.handleInputChange}
          filteredLabels={this.filterLabels(searchTerm)}
          onAddLabel={this.handleAddLabel}
          visible={isPopoverVisible}
        />
      </ClickOutside>
    )
  }

  private get noLabelsIndicator(): JSX.Element {
    const {selectedLabels} = this.props

    if (selectedLabels.length) {
      return
    }

    const handleClick = (): void => {
      if (this.popoverTrigger.current) {
        this.popoverTrigger.current.click()
      }
    }

    return (
      <div
        className="cf-label cf-label--xs cf-label--colorless"
        onClick={handleClick}
        data-testid="inline-labels--empty"
      >
        <span className="cf-label--name">Add a label</span>
      </div>
    )
  }

  private handleAddPopoverEventListener = (): void => {
    if (!this.popoverTrigger.current) {
      return
    }
    this.popoverTrigger.current.addEventListener('click', () => {
      this.setState({
        isPopoverVisible: true,
      })
    })
  }

  private onClickOutside = (e: MouseEvent): void => {
    if (e.target === this.popoverTrigger.current) {
      return
    }

    this.setState({
      isPopoverVisible: false,
    })
  }

  private handleAddLabel = async (labelID: string) => {
    const {onAddLabel, labels} = this.props

    const label = labels.find(label => label.id === labelID)

    if (label) {
      this.selectAvailableItem()
      await onAddLabel(label)
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

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const searchTerm = e.target.value
    const filteredLabels = this.filterLabels(searchTerm)
    if (filteredLabels.length) {
      const selectedItemID = filteredLabels[0].id
      this.setState({searchTerm, selectedItemID})
    } else {
      this.setState({searchTerm})
    }
  }

  private filterLabels = (searchTerm: string): Label[] => {
    const filteredLabels = this.availableLabels.filter(label => {
      const lowercaseName = label.name.toLowerCase()
      const lowercaseSearchTerm = searchTerm.toLowerCase()

      return lowercaseName.includes(lowercaseSearchTerm)
    })

    const searchTermHasExactMatch = filteredLabels.reduce(
      (acc: boolean, current: Label) => {
        return acc === true || current.name === searchTerm
      },
      false
    )

    if (!searchTermHasExactMatch && searchTerm) {
      return this.filteredLabelsWithAddButton(filteredLabels)
    }

    return this.filteredLabelsWithoutAddButton(filteredLabels)
  }

  private filteredLabelsWithAddButton = (filteredLabels: Label[]): Label[] => {
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
    filteredLabels: Label[]
  ): Label[] => {
    return filteredLabels.filter(label => label.id !== ADD_NEW_LABEL_ITEM_ID)
  }

  private get availableLabels(): Label[] {
    const {selectedLabels, labels} = this.props

    return _.differenceBy(labels, selectedLabels, label => label.name)
  }

  private handleCreateLabel = async (label: Label) => {
    const {onCreateLabel, onAddLabel} = this.props
    const {name, properties} = label

    await onCreateLabel(name, properties)
    const newLabel = this.props.labels.find(l => l.name === label.name)
    onAddLabel(newLabel)
  }

  private handleStartCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Open, isPopoverVisible: false})
  }

  private handleStopCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Closed, searchTerm: ''})
  }

  private handleEnsureUniqueLabelName = (name: string): string | null => {
    const {labels} = this.props
    const names = labels.map(label => label.name)

    return validateLabelUniqueness(names, name)
  }
}

const mdtp = {
  onCreateLabel: createLabel,
}

const connector = connect(null, mdtp)

export default connector(InlineLabelsEditor)
