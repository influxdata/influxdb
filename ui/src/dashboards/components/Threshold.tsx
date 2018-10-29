import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import ColorDropdown from 'src/shared/components/ColorDropdown'
import {THRESHOLD_COLORS} from 'src/shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Color, ThresholdColor} from 'src/types/colors'

interface Props {
  visualizationType: string
  threshold: Color
  disableMaxColor: boolean
  onChooseColor: (threshold: Color) => void
  onValidateColorValue: (threshold: Color, targetValue: number) => boolean
  onUpdateColorValue: (threshold: Color, targetValue: number) => void
  onDeleteThreshold: (threshold: Color) => void
  isMin: boolean
  isMax: boolean
}

interface State {
  workingValue: number | string
  valid: boolean
}

@ErrorHandling
class Threshold extends PureComponent<Props, State> {
  private thresholdInputRef: HTMLInputElement

  constructor(props) {
    super(props)

    this.state = {
      workingValue: this.props.threshold.value,
      valid: true,
    }
  }

  public render() {
    const {disableMaxColor, isMax} = this.props
    const {workingValue} = this.state

    return (
      <div className="threshold-item">
        <div className={this.labelClass}>{this.label}</div>
        {this.canBeDeleted ? (
          <button
            className="btn btn-default btn-sm btn-square"
            onClick={this.handleDelete}
          >
            <span className="icon remove" />
          </button>
        ) : null}
        <input
          value={workingValue}
          className={this.inputClass}
          type="number"
          onChange={this.handleChangeWorkingValue}
          onBlur={this.handleBlur}
          onKeyUp={this.handleKeyUp}
          ref={this.handleInputRef}
        />
        <ColorDropdown
          colors={THRESHOLD_COLORS}
          selected={this.selectedColor}
          onChoose={this.handleChooseColor}
          disabled={isMax && disableMaxColor}
        />
      </div>
    )
  }

  private handleChooseColor = (color: ThresholdColor): void => {
    const {onChooseColor, threshold} = this.props
    const {hex, name} = color

    onChooseColor({...threshold, hex, name})
  }

  private get selectedColor(): Color {
    const {
      threshold: {hex, name, type, value, id},
    } = this.props
    return {hex, name, type, value, id}
  }

  private get inputClass(): string {
    const {valid} = this.state

    const inputClass = valid
      ? 'form-control input-sm threshold-item--input'
      : 'form-control input-sm threshold-item--input form-volcano'

    return inputClass
  }

  private get canBeDeleted(): boolean {
    const {visualizationType, isMax, isMin} = this.props

    let canBeDeleted = true

    if (visualizationType === 'gauge') {
      canBeDeleted = !(isMin || isMax)
    }

    return canBeDeleted
  }

  private get labelClass(): string {
    const {visualizationType, isMax, isMin} = this.props

    let labelClass = 'threshold-item--label__editable'

    if (visualizationType === 'gauge') {
      labelClass =
        isMin || isMax
          ? 'threshold-item--label'
          : 'threshold-item--label__editable'
    }

    return labelClass
  }

  private get label(): string {
    let label = 'Threshold'
    const {visualizationType, isMax, isMin} = this.props

    if (isMin && visualizationType === 'gauge') {
      label = 'Minimum'
    }
    if (isMax && visualizationType === 'gauge') {
      label = 'Maximum'
    }

    return label
  }

  private handleChangeWorkingValue = (e: ChangeEvent<HTMLInputElement>) => {
    const {threshold, onValidateColorValue} = this.props
    const targetValue = e.target.value

    const valid = onValidateColorValue(threshold, Number(targetValue))

    this.setState({valid, workingValue: targetValue})
  }

  private handleBlur = () => {
    const {valid, workingValue} = this.state
    const {threshold, onUpdateColorValue} = this.props

    if (valid) {
      onUpdateColorValue(threshold, Number(workingValue))
    } else {
      this.setState({workingValue: threshold.value, valid: true})
    }
  }

  private handleKeyUp = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      this.thresholdInputRef.blur()
    }
  }

  private handleInputRef = (ref: HTMLInputElement) => {
    this.thresholdInputRef = ref
  }

  private handleDelete = () => {
    const {threshold, onDeleteThreshold} = this.props
    onDeleteThreshold(threshold)
  }
}

export default Threshold
