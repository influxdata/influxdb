// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Affixes from 'src/timeMachine/components/view_options/Affixes'
import DecimalPlacesOption from 'src/timeMachine/components/view_options/DecimalPlaces'
import ThresholdsSettings from 'src/shared/components/ThresholdsSettings'

// Actions
import {
  setDecimalPlaces,
  setPrefix,
  setTickPrefix,
  setSuffix,
  setTickSuffix,
  setColors,
} from 'src/timeMachine/actions'

// Types
import {ViewType} from 'src/types'
import {DecimalPlaces} from 'src/types/dashboards'
import {Color} from 'src/types/colors'

interface OwnProps {
  type: ViewType
  colors: Color[]
  decimalPlaces?: DecimalPlaces
  prefix: string
  tickPrefix: string
  suffix: string
  tickSuffix: string
}

interface DispatchProps {
  onUpdatePrefix: (prefix: string) => void
  onUpdateTickPrefix: (tickPrefix: string) => void
  onUpdateSuffix: (suffix: string) => void
  onUpdateTickSuffix: (tickSuffix: string) => void
  onUpdateDecimalPlaces: (decimalPlaces: DecimalPlaces) => void
  onUpdateColors: (colors: Color[]) => void
}

type Props = OwnProps & DispatchProps

class GaugeOptions extends PureComponent<Props> {
  public render() {
    const {
      prefix,
      tickPrefix,
      suffix,
      tickSuffix,
      onUpdatePrefix,
      onUpdateTickPrefix,
      onUpdateSuffix,
      onUpdateTickSuffix,
      onUpdateColors,
    } = this.props

    return (
      <>
        <Grid.Column>
          <h4 className="view-options--header">Customize Gauge</h4>
        </Grid.Column>
        <Affixes
          prefix={prefix}
          tickPrefix={tickPrefix}
          suffix={suffix}
          tickSuffix={tickSuffix}
          onUpdatePrefix={onUpdatePrefix}
          onUpdateTickPrefix={onUpdateTickPrefix}
          onUpdateSuffix={onUpdateSuffix}
          onUpdateTickSuffix={onUpdateTickSuffix}
        />
        {this.decimalPlaces}
        <Grid.Column>
          <h4 className="view-options--header">Colorized Thresholds</h4>
        </Grid.Column>
        <Grid.Column>
          <ThresholdsSettings
            thresholds={this.props.colors}
            onSetThresholds={onUpdateColors}
          />
        </Grid.Column>
      </>
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
}

const mdtp: DispatchProps = {
  onUpdatePrefix: setPrefix,
  onUpdateTickPrefix: setTickPrefix,
  onUpdateSuffix: setSuffix,
  onUpdateTickSuffix: setTickSuffix,
  onUpdateDecimalPlaces: setDecimalPlaces,
  onUpdateColors: setColors,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(GaugeOptions)
