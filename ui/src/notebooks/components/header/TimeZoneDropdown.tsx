import React, {FC, useCallback} from 'react'

import {AppSettingContext} from 'src/notebooks/context/app'

import {TimeZoneDropdown as StatelessTimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'

const TimeZoneDropdown:FC = React.memo(() => {
  const {timeZone, onSetTimeZone} = useContext(AppSettingContext)

  return <StatelessTimeZoneDropdown timeZone={timeZone} onSetTimeZone={onSetTimeZone} />
})

export default TimeZoneDropdown
