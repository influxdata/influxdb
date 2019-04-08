// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Affixes from 'src/timeMachine/components/view_options/Affixes'
import DecimalPlacesOption from 'src/timeMachine/components/view_options/DecimalPlaces'
import ThresholdList from 'src/timeMachine/components/view_options/ThresholdList'
import ThresholdColoring from 'src/timeMachine/components/view_options/ThresholdColoring'

// Actions
import {
  setDecimalPlaces,
  setPrefix,
  setSuffix,
  setColors,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Constants
import {THRESHOLD_TYPE_BASE} from 'src/shared/constants/thresholds'

// Types
import {AppState, NewView} from 'src/types'
import {SingleStatView} from 'src/types/dashboards'
import {DecimalPlaces} from 'src/types/dashboards'
import {Color, ThresholdConfig} from 'src/types/colors'

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

  const colorConfigs = colors
    .filter(c => c.type !== 'scale')
    .map(color => {
      const isBase = color.id === THRESHOLD_TYPE_BASE

      const config: ThresholdConfig = {
        color,
        isBase,
      }

      if (isBase) {
        config.label = 'Base'
      }

      return config
    })

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
      <ThresholdList
        colorConfigs={colorConfigs}
        onUpdateColors={onSetColors}
        onValidateNewColor={() => true}
      />
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
