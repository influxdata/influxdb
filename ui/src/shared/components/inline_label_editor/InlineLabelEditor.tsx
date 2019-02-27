// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Components
import {Input} from 'src/clockface'
import InlineLabelToggle from 'src/shared/components/inline_label_editor/InlineLabelToggle'
import InlineLabelEditorMenu from 'src/shared/components/inline_label_editor/InlineLabelEditorMenu'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import CreateLabelOverlay from 'src/configuration/components/CreateLabelOverlay'

// Types
import {IconFont} from 'src/clockface/types'
import {Label} from 'src/types/v2/labels'

// Utils
import {validateLabelName} from 'src/configuration/utils/labels'

// Styles
import 'src/shared/components/inline_label_editor/InlineLabelEditor.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

enum OverlayState {
  Visible = 'visible',
  Hidden = 'hidden',
}

interface Props {
  selectedLabels: Label[]
  labels: Label[]
  onAddLabel: (label: Label) => void
  onCreateLabel: (labelName: string) => void
}

interface State {
  filterValue: string
  filteredLabels: Label[]
  isSuggesting: boolean
  highlightedID: string
  isCreatingLabel: OverlayState
}

@ErrorHandling
class InlineLabelEditor extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    const initialFilteredLabels = _.differenceBy(
      props.labels,
      props.selectedLabels,
      label => label.name
    )

    this.state = {
      highlightedID: null,
      filterValue: '',
      filteredLabels: initialFilteredLabels,
      isSuggesting: false,
      isCreatingLabel: OverlayState.Hidden,
    }
  }

  public render() {
    const {filterValue, isSuggesting} = this.state

    if (isSuggesting) {
      return (
        <>
          <div className="inline-label-editor">
            <InlineLabelToggle onClick={this.handleStopSuggesting} />
            <ClickOutside onClickOutside={this.handleStopSuggesting}>
              <div className="inline-label-editor--tooltip">
                <h5 className="inline-label-editor--heading">Add Labels</h5>
                <Input
                  icon={IconFont.Search}
                  placeholder="Filter labels..."
                  value={filterValue}
                  onKeyDown={this.handleKeyDown}
                  onChange={this.handleInputChange}
                  autoFocus={true}
                  onBlur={this.handleReFocusInput}
                />
                {this.suggestionMenu}
              </div>
            </ClickOutside>
          </div>
          {this.createLabelOverlay}
        </>
      )
    }

    return (
      <>
        <div className="inline-label-editor">
          <InlineLabelToggle onClick={this.handleStartSuggesting} />
          {!this.props.selectedLabels.length && (
            <div
              className="label label--xs label--colorless"
              onClick={this.handleStartSuggesting}
            >
              Add a label
            </div>
          )}
        </div>
        {this.createLabelOverlay}
      </>
    )
  }

  private get suggestionMenu(): JSX.Element {
    const {labels, selectedLabels} = this.props
    const {isSuggesting, highlightedID, filterValue} = this.state

    const labelsUsed = labels.length === selectedLabels.length

    if (isSuggesting) {
      return (
        <InlineLabelEditorMenu
          filterValue={filterValue}
          allLabelsUsed={labelsUsed}
          filteredLabels={this.availableLabels}
          highlightItemID={highlightedID}
          onItemClick={this.handleAddLabel}
          onItemHighlight={this.handleItemHighlight}
          onStartCreatingLabel={this.handleStartCreatingLabel}
        />
      )
    }
  }

  private handleAddLabel = (labelID: string): void => {
    const {onAddLabel, labels} = this.props

    const label = labels.find(label => label.id === labelID)

    onAddLabel(label)
  }

  private handleItemHighlight = (highlightedID: string): void => {
    this.setState({highlightedID})
  }

  private handleStartSuggesting = () => {
    const {availableLabels} = this
    const {isSuggesting} = this.state

    if (_.isEmpty(availableLabels) && !isSuggesting) {
      return this.setState({
        isSuggesting: true,
        highlightedID: null,
        filterValue: '',
      })
    }

    const highlightedID = this.availableLabels[0].name
    this.setState({isSuggesting: true, highlightedID, filterValue: ''})
  }

  private handleStopSuggesting = () => {
    const {labels: filteredLabels} = this.props

    this.setState({isSuggesting: false, filteredLabels})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    let highlightedID = this.state.highlightedID
    const {labels, selectedLabels} = this.props
    const filterValue = e.target.value

    const availableLabels = _.differenceBy(labels, selectedLabels, l => l.name)

    const filteredLabels = availableLabels.filter(label => {
      return label.name.includes(filterValue)
    })

    const highlightedIDAvailable = filteredLabels.find(
      al => al.name === highlightedID
    )

    if (!highlightedIDAvailable && filteredLabels.length) {
      highlightedID = filteredLabels[0].name
    }

    if (filterValue.length === 0) {
      return this.setState({
        isSuggesting: true,
        filteredLabels: this.props.labels,
        highlightedID: null,
        filterValue: '',
      })
    }

    this.setState({filterValue, filteredLabels, highlightedID})
  }

  private get availableLabels(): Label[] {
    const {selectedLabels} = this.props
    const {filteredLabels} = this.state

    return _.differenceBy(filteredLabels, selectedLabels, label => label.name)
  }

  private handleHighlightAdjacentItem = (direction: ArrowDirection): void => {
    const {highlightedID} = this.state
    const {availableLabels} = this

    if (!availableLabels.length || !highlightedID) {
      return null
    }

    const highlightedIndex = _.findIndex(
      availableLabels,
      label => label.name === highlightedID
    )

    const adjacentIndex = Math.min(
      Math.max(highlightedIndex + direction, 0),
      availableLabels.length - 1
    )

    const adjacentID = availableLabels[adjacentIndex].id

    this.setState({highlightedID: adjacentID})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    const {highlightedID} = this.state

    if (!highlightedID) {
      return
    }

    switch (e.key) {
      case 'Escape':
        return this.handleStopSuggesting()
      case 'Enter':
        return this.handleAddLabel(highlightedID)
      case 'ArrowUp':
        return this.handleHighlightAdjacentItem(ArrowDirection.Up)
      case 'ArrowDown':
        return this.handleHighlightAdjacentItem(ArrowDirection.Down)
      default:
        break
    }
  }

  private handleCreateLabel = async (label: Label) => {
    const newLabel = await client.labels.create(label.name, label.properties)
    this.props.onAddLabel(newLabel)
  }

  private handleReFocusInput = (e: ChangeEvent<HTMLInputElement>): void => {
    e.target.focus()
  }

  private handleStartCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Visible})
    this.handleStopSuggesting()
  }

  private handleStopCreatingLabel = (): void => {
    this.setState({isCreatingLabel: OverlayState.Hidden})
  }

  private handleEnsureUniqueLabelName = (name: string): string | null => {
    const {labels} = this.props
    const labelNames = labels.map(label => label.name)

    return validateLabelName(labelNames, name)
  }

  private get createLabelOverlay(): JSX.Element {
    const {isCreatingLabel, filterValue} = this.state

    return (
      <CreateLabelOverlay
        isVisible={isCreatingLabel === OverlayState.Visible}
        onDismiss={this.handleStopCreatingLabel}
        overrideDefaultName={filterValue}
        onCreateLabel={this.handleCreateLabel}
        onNameValidation={this.handleEnsureUniqueLabelName}
      />
    )
  }
}

export default InlineLabelEditor
