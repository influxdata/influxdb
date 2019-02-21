// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Components
import {Button} from '@influxdata/clockface'
import Input from 'src/clockface/components/inputs/Input'
import Label from 'src/clockface/components/label/Label'
import LabelContainer from 'src/clockface/components/label/LabelContainer'
import LabelSelectorMenu from 'src/clockface/components/label/LabelSelectorMenu'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {ComponentSize} from 'src/clockface/types'
import {Label as APILabel} from 'src/types/v2/labels'

// Styles
import './LabelSelector.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

interface Props {
  selectedLabels: APILabel[]
  allLabels: APILabel[]
  onAddLabel: (label: APILabel) => void
  onRemoveLabel: (label: APILabel) => void
  onRemoveAllLabels: () => void
  onCreateLabel: (label: APILabel) => void
  resourceType: string
  inputSize?: ComponentSize
}

interface State {
  filterValue: string
  filteredLabels: APILabel[]
  isSuggesting: boolean
  highlightedID: string
}

@ErrorHandling
class LabelSelector extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    inputSize: ComponentSize.Small,
  }

  constructor(props: Props) {
    super(props)

    const initialFilteredLabels = _.differenceBy(
      props.allLabels,
      props.selectedLabels,
      label => label.name
    )

    this.state = {
      highlightedID: null,
      filterValue: '',
      filteredLabels: initialFilteredLabels,
      isSuggesting: false,
    }
  }

  public componentDidMount() {
    this.handleStartSuggesting()
  }

  public render() {
    return (
      <ClickOutside onClickOutside={this.handleStopSuggesting}>
        <div className="label-selector">
          <div className="label-selector--selection">
            {this.selectedLabels}
            {this.clearSelectedButton}
          </div>
          {this.input}
        </div>
      </ClickOutside>
    )
  }

  private get selectedLabels(): JSX.Element {
    const {selectedLabels, resourceType} = this.props

    if (selectedLabels && selectedLabels.length) {
      return (
        <LabelContainer className="label-selector--selected">
          {selectedLabels.map(label => (
            <Label
              key={label.name}
              name={label.name}
              id={label.name}
              colorHex={label.properties.color}
              onDelete={this.handleDelete}
              description={label.properties.description}
            />
          ))}
        </LabelContainer>
      )
    }

    return (
      <div className="label-selector--none-selected">{`${_.upperFirst(
        resourceType
      )} has no labels`}</div>
    )
  }

  private get suggestionMenu(): JSX.Element {
    const {allLabels, selectedLabels} = this.props
    const {isSuggesting, highlightedID, filterValue} = this.state

    const allLabelsUsed = allLabels.length === selectedLabels.length

    if (isSuggesting) {
      return (
        <LabelSelectorMenu
          filterValue={filterValue}
          allLabelsUsed={allLabelsUsed}
          filteredLabels={this.availableLabels}
          highlightItemID={highlightedID}
          onItemClick={this.handleAddLabel}
          onItemHighlight={this.handleItemHighlight}
          onCreateLabel={this.handleCreateLabel}
        />
      )
    }
  }

  private get input(): JSX.Element {
    const {resourceType, inputSize} = this.props
    const {filterValue} = this.state

    return (
      <div className="label-selector--input">
        <Input
          placeholder={`Add labels to ${resourceType}`}
          value={filterValue}
          onFocus={this.handleStartSuggesting}
          onKeyDown={this.handleKeyDown}
          onChange={this.handleInputChange}
          size={inputSize}
          autoFocus={true}
        />
        {this.suggestionMenu}
      </div>
    )
  }

  private handleAddLabel = (labelID: string): void => {
    const {onAddLabel, allLabels} = this.props

    const label = allLabels.find(label => label.name === labelID)

    onAddLabel(label)
    this.handleStopSuggesting()
  }

  private handleItemHighlight = (highlightedID: string): void => {
    this.setState({highlightedID})
  }

  private handleStartSuggesting = () => {
    const {availableLabels} = this

    if (_.isEmpty(availableLabels)) {
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
    const {allLabels: filteredLabels} = this.props

    this.setState({isSuggesting: false, filterValue: '', filteredLabels})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    let highlightedID = this.state.highlightedID
    const {allLabels, selectedLabels} = this.props
    const filterValue = e.target.value

    const availableLabels = _.differenceBy(
      allLabels,
      selectedLabels,
      l => l.name
    )

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
        filteredLabels: this.props.allLabels,
        highlightedID: null,
        filterValue: '',
      })
    }

    this.setState({filterValue, filteredLabels, highlightedID})
  }

  private get availableLabels(): APILabel[] {
    const {selectedLabels} = this.props
    const {filteredLabels} = this.state

    return _.differenceBy(filteredLabels, selectedLabels, label => label.name)
  }

  private handleDelete = (labelID: string): void => {
    const {onRemoveLabel, selectedLabels} = this.props

    const label = selectedLabels.find(l => l.name === labelID)

    onRemoveLabel(label)
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

    const adjacentID = availableLabels[adjacentIndex].name

    this.setState({highlightedID: adjacentID})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    const {highlightedID} = this.state

    if (!highlightedID) {
      return
    }

    switch (e.key) {
      case 'Escape':
        e.currentTarget.blur()
        return this.handleStopSuggesting()
      case 'Enter':
        e.currentTarget.blur()
        return this.handleAddLabel(highlightedID)
      case 'ArrowUp':
        return this.handleHighlightAdjacentItem(ArrowDirection.Up)
      case 'ArrowDown':
        return this.handleHighlightAdjacentItem(ArrowDirection.Down)
      default:
        break
    }
  }

  private get clearSelectedButton(): JSX.Element {
    const {selectedLabels, onRemoveAllLabels} = this.props

    if (_.isEmpty(selectedLabels)) {
      return
    }

    return (
      <Button
        text="Remove All Labels"
        size={ComponentSize.ExtraSmall}
        customClass="label-selector--remove-all"
        onClick={onRemoveAllLabels}
      />
    )
  }

  private handleCreateLabel = async (label: APILabel) => {
    const newLabel = await client.labels.create(label.name, label.properties)
    this.props.onAddLabel(newLabel)
    this.handleStopSuggesting()
  }
}

export default LabelSelector
