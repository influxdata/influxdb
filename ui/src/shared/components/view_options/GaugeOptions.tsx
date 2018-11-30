// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import Form from 'src/clockface/components/form_layout/Form'
import Affixes from 'src/shared/components/view_options/options/Affixes'
import DecimalPlacesOption from 'src/shared/components/view_options/options/DecimalPlaces'
import ThresholdList from 'src/shared/components/view_options/options/ThresholdList'

// Actions
import {
  setDecimalPlaces,
  setPrefix,
  setSuffix,
  setColors,
} from 'src/shared/actions/v2/timeMachines'

// Types
import {ViewType} from 'src/types/v2'
import {DecimalPlaces} from 'src/types/v2/dashboards'
import {Color, ColorConfig} from 'src/types/colors'

interface OwnProps {
  type: ViewType
  colors: Color[]
  decimalPlaces?: DecimalPlaces
  prefix: string
  suffix: string
}

interface DispatchProps {
  onUpdatePrefix: (prefix: string) => void
  onUpdateSuffix: (suffix: string) => void
  onUpdateDecimalPlaces: (decimalPlaces: DecimalPlaces) => void
  onUpdateColors: (colors: Color[]) => void
}

type Props = OwnProps & DispatchProps

class GaugeOptions extends PureComponent<Props> {
  public render() {
    const {
      prefix,
      suffix,
      onUpdatePrefix,
      onUpdateSuffix,
      onUpdateColors,
    } = this.props

    return (
      <Form>
        <ThresholdList
          colorConfigs={this.colorConfigs}
          onUpdateColors={onUpdateColors}
          onValidateNewColor={this.handleValidateNewColor}
        />
        <Affixes
          prefix={prefix}
          suffix={suffix}
          onUpdatePrefix={onUpdatePrefix}
          onUpdateSuffix={onUpdateSuffix}
        />
        {this.decimalPlaces}
      </Form>
    )
  }

  private get decimalPlaces(): JSX.Element {
    const {onUpdateDecimalPlaces, decimalPlaces} = this.props

    if (!decimalPlaces) {
      return null
    }

    return (
      <DecimalPlacesOption
        digits={decimalPlaces.digits}
        isEnforced={decimalPlaces.isEnforced}
        onDecimalPlacesChange={onUpdateDecimalPlaces}
      />
    )
  }

  private get colorConfigs(): ColorConfig[] {
    const {maxColor, minColor} = this.extents
    const {colors} = this.props

    return colors.map(color => {
      switch (color.id) {
        case minColor.id:
          return {
            color,
            isDeletable: false,
            disableColor: false,
            label: 'Minimum',
          }
        case maxColor.id:
          return {
            color,
            isDeletable: false,
            disableColor: colors.length > 2,
            label: 'Maximum',
          }
        default:
          return {color}
      }
    })
  }

  private get extents(): {minColor: Color; maxColor: Color} {
    const first = this.props.colors[0]

    const defaults = {minColor: first, maxColor: first}

    return this.props.colors.reduce((extents, color) => {
      if (!extents.minColor || extents.minColor.value > color.value) {
        return {...extents, minColor: color}
      } else if (!extents.minColor || extents.maxColor.value < color.value) {
        return {...extents, maxColor: color}
      }

      return extents
    }, defaults)
  }

  private handleValidateNewColor = (sortedColors: Color[], newColor: Color) => {
    const newColorValue = newColor.value
    let allowedToUpdate = false

    const minValue = sortedColors[0].value
    const maxValue = sortedColors[sortedColors.length - 1].value

    if (newColorValue === minValue) {
      const nextValue = sortedColors[1].value
      allowedToUpdate = newColorValue < nextValue
    } else if (newColorValue === maxValue) {
      const previousValue = sortedColors[sortedColors.length - 2].value
      allowedToUpdate = previousValue < newColorValue
    } else {
      const greaterThanMin = newColorValue > minValue
      const lessThanMax = newColorValue < maxValue

      const colorsWithoutMinOrMax = sortedColors.slice(
        1,
        sortedColors.length - 1
      )

      const isUnique = !colorsWithoutMinOrMax.some(
        color => color.value === newColorValue && color.id !== newColor.id
      )

      allowedToUpdate = greaterThanMin && lessThanMax && isUnique
    }

    return allowedToUpdate
  }
}

const mdtp: DispatchProps = {
  onUpdatePrefix: setPrefix,
  onUpdateSuffix: setSuffix,
  onUpdateDecimalPlaces: setDecimalPlaces,
  onUpdateColors: setColors,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(GaugeOptions)
