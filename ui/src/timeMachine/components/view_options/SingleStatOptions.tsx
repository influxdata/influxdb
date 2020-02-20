// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Affixes from 'src/timeMachine/components/view_options/Affixes'
import DecimalPlacesOption from 'src/timeMachine/components/view_options/DecimalPlaces'
import ThresholdColoring from 'src/timeMachine/components/view_options/ThresholdColoring'
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

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {
  AppState,
  NewView,
  Color,
  SingleStatViewProperties,
  DecimalPlaces,
} from 'src/types'

interface StateProps {
  colors: Color[]
  prefix: string
  tickPrefix: string
  suffix: string
  tickSuffix: string
  decimalPlaces: DecimalPlaces
}

interface DispatchProps {
  onSetPrefix: typeof setPrefix
  onSetTickPrefix: typeof setTickPrefix
  onSetSuffix: typeof setSuffix
  onSetTickSuffix: typeof setTickSuffix
  onSetDecimalPlaces: typeof setDecimalPlaces
  onSetColors: typeof setColors
}

type Props = StateProps & DispatchProps

const SingleStatOptions: SFC<Props> = props => {
  const {
    colors,
    prefix,
    tickPrefix,
    suffix,
    tickSuffix,
    decimalPlaces,
    onSetPrefix,
    onSetTickPrefix,
    onSetSuffix,
    onSetTickSuffix,
    onSetDecimalPlaces,
    onSetColors,
  } = props

  return (
    <>
      <Grid.Column>
        <h4 className="view-options--header">Customize Single-Stat</h4>
      </Grid.Column>
      <Affixes
        prefix={prefix}
        tickPrefix={tickPrefix}
        suffix={suffix}
        type="single-stat"
        tickSuffix={tickSuffix}
        onUpdatePrefix={onSetPrefix}
        onUpdateTickPrefix={onSetTickPrefix}
        onUpdateSuffix={onSetSuffix}
        onUpdateTickSuffix={onSetTickSuffix}
      />
      {decimalPlaces && (
        <DecimalPlacesOption
          digits={decimalPlaces.digits}
          isEnforced={decimalPlaces.isEnforced}
          onDecimalPlacesChange={onSetDecimalPlaces}
        />
      )}
      <Grid.Column>
        <h4 className="view-options--header">Colorized Thresholds</h4>
      </Grid.Column>
      <Grid.Column>
        <ThresholdsSettings thresholds={colors} onSetThresholds={onSetColors} />
      </Grid.Column>
      <Grid.Column>
        <ThresholdColoring />
      </Grid.Column>
    </>
  )
}

const mstp = (state: AppState) => {
  const view = getActiveTimeMachine(state).view as NewView<
    SingleStatViewProperties
  >
  const {
    colors,
    prefix,
    suffix,
    decimalPlaces,
    tickPrefix,
    tickSuffix,
  } = view.properties

  return {colors, prefix, suffix, decimalPlaces, tickPrefix, tickSuffix}
}

const mdtp: DispatchProps = {
  onSetPrefix: setPrefix,
  onSetTickPrefix: setTickPrefix,
  onSetSuffix: setSuffix,
  onSetTickSuffix: setTickSuffix,
  onSetDecimalPlaces: setDecimalPlaces,
  onSetColors: setColors,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(SingleStatOptions)
