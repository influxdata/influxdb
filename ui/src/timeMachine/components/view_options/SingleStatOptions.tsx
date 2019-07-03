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
  setSuffix,
  setColors,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {
  AppState,
  NewView,
  Color,
  SingleStatView,
  DecimalPlaces,
} from 'src/types'

interface StateProps {
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
}

interface DispatchProps {
  onSetPrefix: typeof setPrefix
  onSetSuffix: typeof setSuffix
  onSetDecimalPlaces: typeof setDecimalPlaces
  onSetColors: typeof setColors
}

type Props = StateProps & DispatchProps

const SingleStatOptions: SFC<Props> = props => {
  const {
    colors,
    prefix,
    suffix,
    decimalPlaces,
    onSetPrefix,
    onSetSuffix,
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
        suffix={suffix}
        onUpdatePrefix={onSetPrefix}
        onUpdateSuffix={onSetSuffix}
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
      <ThresholdsSettings thresholds={colors} onSetThresholds={onSetColors} />
      <Grid.Column>
        <ThresholdColoring />
      </Grid.Column>
    </>
  )
}

const mstp = (state: AppState) => {
  const view = getActiveTimeMachine(state).view as NewView<SingleStatView>
  const {colors, prefix, suffix, decimalPlaces} = view.properties

  return {colors, prefix, suffix, decimalPlaces}
}

const mdtp: DispatchProps = {
  onSetPrefix: setPrefix,
  onSetSuffix: setSuffix,
  onSetDecimalPlaces: setDecimalPlaces,
  onSetColors: setColors,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(SingleStatOptions)
