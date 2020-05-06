// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {SelectDropdown, IconFont} from '@influxdata/clockface'

// Actions
import {setTimeZone} from 'src/shared/actions/app'

// Constants
import {TIME_ZONES} from 'src/shared/constants/timeZones'

// Types
import {AppState, TimeZone} from 'src/types'

interface StateProps {
  timeZone: TimeZone
}

interface DispatchProps {
  onSetTimeZone: typeof setTimeZone
}

type Props = StateProps & DispatchProps

const TimeZoneDropdown: FunctionComponent<Props> = ({
  timeZone: selectedTimeZone,
  onSetTimeZone,
}) => {
  return (
    <SelectDropdown
      options={TIME_ZONES.map(tz => tz.timeZone)}
      selectedOption={selectedTimeZone}
      onSelect={onSetTimeZone}
      buttonIcon={IconFont.Annotate}
      style={{width: '115px'}}
    />
  )
}

export {TimeZoneDropdown}

const mstp = (state: AppState): StateProps => {
  return {timeZone: state.app.persisted.timeZone || 'Local'}
}

const mdtp = {onSetTimeZone: setTimeZone}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeZoneDropdown)
