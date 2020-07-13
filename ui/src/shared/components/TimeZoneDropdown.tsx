// Libraries
import React, {FunctionComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {SelectDropdown, IconFont} from '@influxdata/clockface'

// Actions & Selectors
import {setTimeZone} from 'src/shared/actions/app'
import {getTimeZone} from 'src/dashboards/selectors'

// Constants
import {TIME_ZONES} from 'src/shared/constants/timeZones'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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

const mstp = (state: AppState) => {
  return {timeZone: getTimeZone(state)}
}

const mdtp = {onSetTimeZone: setTimeZone}

const connector = connect(mstp, mdtp)

export default connector(TimeZoneDropdown)
