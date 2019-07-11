// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Dropdown, IconFont} from '@influxdata/clockface'

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
    <Dropdown
      selectedID={selectedTimeZone}
      onChange={onSetTimeZone}
      icon={IconFont.Annotate}
      widthPixels={115}
    >
      {TIME_ZONES.map(({timeZone, displayName}) => (
        <Dropdown.Item key={timeZone} id={timeZone} value={timeZone}>
          {displayName}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

const mstp = (state: AppState): StateProps => {
  return {timeZone: state.app.persisted.timeZone || 'Local'}
}

const mdtp = {onSetTimeZone: setTimeZone}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeZoneDropdown)
