import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import {
  Button,
  ButtonShape,
  IconFont,
  Input,
  InputType,
  ComponentStatus,
  ButtonType,
} from 'src/clockface'

import ColorDropdown from 'src/shared/components/ColorDropdown'
import {THRESHOLD_COLORS} from 'src/shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Color, ColorLabel} from 'src/types/colors'
import {SeverityColor, SeverityColorOptions} from 'src/types/logs'

interface Props {
  label?: string
  threshold: Color
  isDeletable?: boolean
  disableColor?: boolean
  onChooseColor: (threshold: Color) => void
  onValidateColorValue: (threshold: Color, targetValue: number) => boolean
  onUpdateColorValue: (threshold: Color, targetValue: number) => void
  onDeleteThreshold: (threshold: Color) => void
}

interface State {
  workingValue: number | string
  valid: boolean
}

@ErrorHandling
class Threshold extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    label: 'Threshold',
    disableColor: false,
    isDeletable: true,
  }

  constructor(props) {
    super(props)

    this.state = {
      workingValue: this.props.threshold.value,
      valid: true,
    }
  }

  public render() {
    const {isDeletable, disableColor} = this.props
    const {workingValue} = this.state

    return (
      <div className="threshold-item">
        <div className={this.labelClass}>{this.props.label}</div>
        {isDeletable ? (
          <Button
            shape={ButtonShape.Square}
            onClick={this.handleDelete}
            icon={IconFont.Remove}
            type={ButtonType.Button}
          />
        ) : null}
        <Input
          value={workingValue.toString()}
          customClass="threshold-item--input"
          type={InputType.Number}
          onChange={this.handleChangeWorkingValue}
          onBlur={this.handleBlur}
          onKeyUp={this.handleKeyUp}
          status={this.inputStatus}
        />
        <ColorDropdown
          colors={THRESHOLD_COLORS}
          selected={this.selectedColor}
          onChoose={this.handleChooseColor}
          disabled={disableColor}
        />
      </div>
    )
  }

  private handleChooseColor = (color: ColorLabel): void => {
    const {onChooseColor, threshold} = this.props
    const {hex, name} = color

    onChooseColor({...threshold, hex, name})
  }

  private get selectedColor(): SeverityColor {
    const {
      threshold: {hex, name},
    } = this.props

    const colorName = name as SeverityColorOptions

    return {hex, name: colorName}
  }

  private get inputStatus(): ComponentStatus {
    const {valid} = this.state

    if (!valid) {
      return ComponentStatus.Error
    }

    return ComponentStatus.Valid
  }

  private get labelClass(): string {
    if (this.props.isDeletable) {
      return 'threshold-item--label__editable'
    } else {
      return 'threshold-item--label'
    }
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
      e.currentTarget.blur()
    }
  }

  private handleDelete = () => {
    const {threshold, onDeleteThreshold} = this.props
    onDeleteThreshold(threshold)
  }
}

export default Threshold
