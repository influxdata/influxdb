// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

// Components
import Input from 'src/clockface/components/inputs/Input'
import Button from 'src/clockface/components/Button'
import Label, {LabelType} from 'src/clockface/components/label/Label'
import LabelContainer from 'src/clockface/components/label/LabelContainer'
import LabelSelectorMenu from 'src/clockface/components/label/LabelSelectorMenu'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {ComponentSize} from 'src/clockface/types'

// Styles
import './LabelSelector.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

enum ArrowDirection {
  Up = -1,
  Down = 1,
}

interface Props {
  selectedLabels: LabelType[]
  allLabels: LabelType[]
  onAddLabel: (label: LabelType) => void
  onRemoveLabel: (label: LabelType) => void
  onRemoveAllLabels: () => void
  resourceType: string
  inputSize?: ComponentSize
}

interface State {
  filterValue: string
  filteredLabels: LabelType[]
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

  public render() {
    const {resourceType} = this.props
    const {filterValue} = this.state

    return (
      <div className="label-selector">
        <ClickOutside onClickOutside={this.handleStopSuggesting}>
          <div className="label-selector--input">
            <Input
              placeholder={`Add labels to ${resourceType}`}
              value={filterValue}
              onFocus={this.handleStartSuggesting}
              onKeyDown={this.handleKeyDown}
              onChange={this.handleInputChange}
            />
            {this.suggestionMenu}
          </div>
        </ClickOutside>
        <div className="label-selector--bottom">
          {this.selectedLabels}
          {this.clearSelectedButton}
        </div>
      </div>
    )
  }

  private get selectedLabels(): JSX.Element {
    const {selectedLabels, resourceType} = this.props

    if (selectedLabels && selectedLabels.length) {
      return (
        <LabelContainer className="label-selector--selected">
          {selectedLabels.map(label => (
            <Label
              key={label.id}
              name={label.name}
              id={label.id}
              colorHex={label.colorHex}
              onDelete={this.handleDelete}
              description={label.description}
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
    const {isSuggesting, highlightedID} = this.state

    const allLabelsUsed = allLabels.length === selectedLabels.length

    if (isSuggesting) {
      return (
        <LabelSelectorMenu
          allLabelsUsed={allLabelsUsed}
          filteredLabels={this.availableLabels}
          highlightItemID={highlightedID}
          onItemClick={this.handleAddLabel}
          onItemHighlight={this.handleItemHighlight}
        />
      )
    }
  }

  private handleAddLabel = (labelID: string): void => {
    const {onAddLabel, allLabels} = this.props

    const label = allLabels.find(label => label.id === labelID)

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

    const highlightedID = this.availableLabels[0].id
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
      const filterChars = _.lowerCase(filterValue)
        .replace(/\s/g, '')
        .split('')
      const labelChars = _.lowerCase(label.name)
        .replace(/\s/g, '')
        .split('')

      const overlap = _.difference(filterChars, labelChars)

      if (overlap.length) {
        return false
      }

      return true
    })

    const highlightedIDAvailable = filteredLabels.find(
      al => al.id === highlightedID
    )

    if (!highlightedIDAvailable && filteredLabels.length) {
      highlightedID = filteredLabels[0].id
    }

    this.setState({filterValue, filteredLabels, highlightedID})
  }

  private get availableLabels(): LabelType[] {
    const {selectedLabels} = this.props
    const {filteredLabels} = this.state

    return _.differenceBy(filteredLabels, selectedLabels, label => label.name)
  }

  private handleDelete = (labelID: string): void => {
    const {onRemoveLabel, selectedLabels} = this.props

    const label = selectedLabels.find(l => l.id === labelID)

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
      label => label.id === highlightedID
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
}

export default LabelSelector
