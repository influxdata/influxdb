import React, {FC, useMemo} from 'react'
import {default as StatelessAutoRefreshDropdown} from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {TimeContextProps} from 'src/notebooks/components/header/Buttons'
import {TimeBlock} from 'src/notebooks/context/time'
import {AutoRefreshStatus} from 'src/types'

// Utils
import {event} from 'src/notebooks/shared/event'

const AutoRefreshDropdown: FC<TimeContextProps> = ({context, update}) => {
  const {refresh} = context

  const updateRefresh = (interval: number) => {
    const status =
      interval === 0 ? AutoRefreshStatus.Paused : AutoRefreshStatus.Active

    event('Auto Refresh Updated', {
      interval: '' + interval,
    })

    update({
      refresh: {
        status,
        interval,
      },
    } as TimeBlock)
  }

  return useMemo(
    () => (
      <StatelessAutoRefreshDropdown
        selected={refresh}
        onChoose={updateRefresh}
        showManualRefresh={false}
      />
    ),
    [refresh]
  )
}

export default AutoRefreshDropdown
