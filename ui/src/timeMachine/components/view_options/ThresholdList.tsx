// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import uuid from 'uuid'

// Components
import ThresholdItem from 'src/timeMachine/components/view_options/ThresholdItem'
import {
  Form,
  Button,
  ComponentSize,
  IconFont,
  ComponentStatus,
  ButtonType,
  Grid,
} from 'src/clockface'

// Constants
import {
  COLOR_TYPE_THRESHOLD,
  THRESHOLD_COLORS,
  MAX_THRESHOLDS,
  DEFAULT_VALUE_MAX,
} from 'src/shared/constants/thresholds'

// Styles
import 'src/timeMachine/components/view_options/ThresholdList.scss'

// Types
import {Color, ThresholdConfig} from 'src/types/colors'

interface Props {
  colorConfigs: ThresholdConfig[]
  onUpdateColors: (colors: Color[]) => void
  onValidateNewColor: (colors: Color[], newColor: Color) => boolean
}

class ThresholdList extends PureComponent<Props> {
  public render() {
    return (
      <Grid.Column>
        <Form.Element label="Thresholds">
          <div className="threshold-list">
            <Button
              size={ComponentSize.Small}
              onClick={this.handleAddThreshold}
              status={this.disableAddThreshold}
              icon={IconFont.Plus}
              type={ButtonType.Button}
              text="Add a Threshold"
            />
            {this.sortedColorConfigs.map<JSX.Element>(colorConfig => {
              const {
                color: threshold,
                isDeletable,
                isBase,
                disableColor,
                label,
              } = colorConfig

              return (
                <ThresholdItem
                  label={label}
                  key={uuid.v4()}
                  threshold={threshold}
                  isBase={isBase}
                  isDeletable={isDeletable}
                  disableColor={disableColor}
                  onChooseColor={this.handleChooseColor}
                  onDeleteThreshold={this.handleDeleteThreshold}
                  onUpdateColorValue={this.handleUpdateColorValue}
                  onValidateColorValue={this.handleValidateColorValue}
                />
              )
            })}
          </div>
        </Form.Element>
      </Grid.Column>
    )
  }

  private handleAddThreshold = () => {
    const sortedColors = this.sortedColorConfigs.map(config => config.color)

    if (sortedColors.length <= MAX_THRESHOLDS) {
      const randomColor = _.random(0, THRESHOLD_COLORS.length - 1)

      const maxColor = sortedColors[sortedColors.length - 1]

      let maxValue = DEFAULT_VALUE_MAX

      if (sortedColors.length > 1) {
        maxValue = maxColor.value
      }

      const minValue = sortedColors[0].value

      const randomValue = _.round(_.random(minValue, maxValue, true), 2)

      const color: Color = {
        type: COLOR_TYPE_THRESHOLD,
        id: uuid.v4(),
        value: randomValue,
        hex: THRESHOLD_COLORS[randomColor].hex,
        name: THRESHOLD_COLORS[randomColor].name,
      }

      const updatedColors = _.sortBy<Color>(
        [...sortedColors, color],
        color => color.value
      )

      this.props.onUpdateColors(updatedColors)
    }
  }

  private handleChooseColor = (threshold: Color) => {
    const colors = this.props.colorConfigs.map(
      ({color}) =>
        color.id === threshold.id
          ? {...color, hex: threshold.hex, name: threshold.name}
          : color
    )

    this.props.onUpdateColors(colors)
  }

  private handleUpdateColorValue = (threshold: Color, value: number) => {
    const colors = this.props.colorConfigs.map(
      ({color}) => (color.id === threshold.id ? {...color, value} : color)
    )

    this.props.onUpdateColors(colors)
  }

  private handleDeleteThreshold = (threshold: Color) => {
    const updatedColors = this.sortedColorConfigs.reduce(
      (colors, {color}) =>
        color.id === threshold.id ? colors : [...colors, color],
      []
    )

    this.props.onUpdateColors(updatedColors)
  }

  private handleValidateColorValue = (newColor: Color) => {
    const {sortedColorConfigs} = this
    const sortedColors = sortedColorConfigs.map(config => config.color)

    return this.props.onValidateNewColor(sortedColors, newColor)
  }

  private get disableAddThreshold(): ComponentStatus {
    if (this.props.colorConfigs.length > MAX_THRESHOLDS) {
      return ComponentStatus.Disabled
    } else {
      return ComponentStatus.Valid
    }
  }

  private get sortedColorConfigs() {
    return _.sortBy(this.props.colorConfigs, config => config.color.value)
  }
}

export default ThresholdList
