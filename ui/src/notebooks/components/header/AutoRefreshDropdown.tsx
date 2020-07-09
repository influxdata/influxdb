import React, {FC, useMemo, useCallback} from 'react'
import {default as StatelessAutoRefreshDropdown} from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {TimeContextProps} from 'src/notebooks/components/header/Buttons'
import {TimeBlock} from 'src/notebooks/context/time'
import {AutoRefreshStatus} from 'src/types'

// Utils
import {event} from 'src/notebooks/shared/event'

const AutoRefreshDropdown: FC<TimeContextProps> = ({context, update}) => {
  const {refresh} = context

  const updateRefresh = useCallback(
    (interval: number) => {
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
    },
    [update]
  )

  return useMemo(() => {
    return (
      <StatelessAutoRefreshDropdown
        selected={refresh}
        onChoose={updateRefresh}
        showManualRefresh={false}
      />
    )
  }, [refresh, updateRefresh])
}

export default AutoRefreshDropdown
