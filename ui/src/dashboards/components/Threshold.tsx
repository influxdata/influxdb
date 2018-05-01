import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import ColorDropdown from 'src/shared/components/ColorDropdown'
import {THRESHOLD_COLORS} from 'src/shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface SelectedColor {
  hex: string
  name: string
}
interface ThresholdProps {
  type: string
  hex: string
  id: string
  name: string
  value: number
}

interface Props {
  visualizationType: string
  threshold: ThresholdProps
  disableMaxColor: boolean
  onChooseColor: (threshold: ThresholdProps) => void
  onValidateColorValue: (
    threshold: ThresholdProps,
    targetValue: number
  ) => boolean
  onUpdateColorValue: (threshold: ThresholdProps, targetValue: number) => void
  onDeleteThreshold: (threshold: ThresholdProps) => void
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
    const {threshold, disableMaxColor, onChooseColor, isMax} = this.props
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
          onChoose={onChooseColor(threshold)}
          disabled={isMax && disableMaxColor}
        />
      </div>
    )
  }

  private get selectedColor(): SelectedColor {
    const {
      threshold: {hex, name},
    } = this.props
    return {hex, name}
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
