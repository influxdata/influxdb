import React, {FC, useContext} from 'react'

import {AppSettingContext} from 'src/notebooks/context/app'

import {TimeZoneDropdown as StatelessTimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'
import {TimeZone} from 'src/types'

// Utils
import {event} from 'src/notebooks/shared/event'

const TimeZoneDropdown: FC = React.memo(() => {
  const {timeZone, onSetTimeZone} = useContext(AppSettingContext)

  const setTimeZone = (tz: TimeZone) => {
    event('Time Zone Changed', {timeZone: tz as string})

    onSetTimeZone(tz)
  }

  return (
    <StatelessTimeZoneDropdown
      timeZone={timeZone}
      onSetTimeZone={setTimeZone}
    />
  )
})

export default TimeZoneDropdown
