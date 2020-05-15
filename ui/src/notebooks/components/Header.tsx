import React, {FC, useContext} from 'react'

import {Page} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider, {AppSettingContext} from 'src/notebooks/context/app'

import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {AutoRefreshStatus, RemoteDataState} from 'src/types'
import {TimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'

const Header: FC<{}> = () => {
  const {id} = useContext(NotebookContext)
  const {timeContext, addTimeContext, updateTimeContext} = useContext(
    TimeContext
  )
  const {timeZone, onSetTimeZone} = useContext(AppSettingContext)

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  const {refresh, range} = timeContext[id]

  function updateRefresh(interval: number) {
    if (interval === 0) {
      updateTimeContext(id, {
        refresh: {
          status: AutoRefreshStatus.Paused,
          interval,
        },
      } as TimeBlock)
    } else {
      updateTimeContext(id, {
        refresh: {
          status: AutoRefreshStatus.Active,
          interval,
        },
      } as TimeBlock)
    }
  }

  function updateRange(range) {
    updateTimeContext(id, {
      ...timeContext[id],
      range,
    })
  }

  function submit() {} // eslint-disable-line @typescript-eslint/no-empty-function

  return (
    <>
      <Page.Title title="Notebooks" />
      <div className="notebook-header--buttons">
        <TimeZoneDropdown timeZone={timeZone} onSetTimeZone={onSetTimeZone} />
        <TimeRangeDropdown timeRange={range} onSetTimeRange={updateRange} />
        <AutoRefreshDropdown
          selected={refresh}
          onChoose={updateRefresh}
          showManualRefresh={false}
        />
        <SubmitQueryButton
          submitButtonDisabled={false}
          queryStatus={RemoteDataState.NotStarted}
          onSubmit={submit}
        />
      </div>
    </>
  )
}

export {Header}

export default () => (
  <TimeProvider>
    <AppSettingProvider>
      <Header />
    </AppSettingProvider>
  </TimeProvider>
)
