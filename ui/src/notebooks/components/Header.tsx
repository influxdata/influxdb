import React, {FC, useContext, useCallback, useMemo} from 'react'

import {Page} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider, {AppSettingContext} from 'src/notebooks/context/app'
import AddButtons from 'src/notebooks/components/AddButtons'

import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {AutoRefreshStatus, RemoteDataState} from 'src/types'
import {TimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'

const FULL_WIDTH = true

const ConnectedTimeZoneDropdown = React.memo(() => {
  const {timeZone, onSetTimeZone} = useContext(AppSettingContext)

  return <TimeZoneDropdown timeZone={timeZone} onSetTimeZone={onSetTimeZone} />
})

const ConnectedTimeRangeDropdown = ({context, update}) => {
  const {range} = context

  const updateRange = range => {
    update({
      range,
    })
  }

  return useMemo(() => {
    return <TimeRangeDropdown timeRange={range} onSetTimeRange={updateRange} />
  }, [range])
}

const ConnectedAutoRefreshDropdown = ({context, update}) => {
  const {refresh} = context

  const updateRefresh = (interval: number) => {
    const status =
      interval === 0 ? AutoRefreshStatus.Paused : AutoRefreshStatus.Active

    update({
      refresh: {
        status,
        interval,
      },
    } as TimeBlock)
  }

  return useMemo(
    () => (
      <AutoRefreshDropdown
        selected={refresh}
        onChoose={updateRefresh}
        showManualRefresh={false}
      />
    ),
    [refresh]
  )
}

const EnsureTimeContextExists: FC = ({children}) => {
  const {id} = useContext(NotebookContext)
  const {timeContext, addTimeContext, updateTimeContext} = useContext(
    TimeContext
  )

  const update = useCallback(
    data => {
      updateTimeContext(id, data)
    },
    [id]
  )

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  return (
    <>
      <ConnectedTimeZoneDropdown />
      <ConnectedTimeRangeDropdown context={timeContext[id]} update={update} />
      <ConnectedAutoRefreshDropdown context={timeContext[id]} update={update} />
    </>
  )
}

const Header: FC = () => {
  function submit() {} // eslint-disable-line @typescript-eslint/no-empty-function

  return (
    <>
      <Page.Header fullWidth={FULL_WIDTH}>
        <Page.Title title="Notebooks" />
      </Page.Header>
      <Page.ControlBar fullWidth={FULL_WIDTH}>
        <Page.ControlBarLeft>
          <AddButtons />
        </Page.ControlBarLeft>
        <Page.ControlBarRight>
          <div className="notebook-header--buttons">
            <EnsureTimeContextExists />
            <SubmitQueryButton
              submitButtonDisabled={false}
              queryStatus={RemoteDataState.NotStarted}
              onSubmit={submit}
            />
          </div>
        </Page.ControlBarRight>
      </Page.ControlBar>
    </>
  )
}

export default () => (
  <TimeProvider>
    <AppSettingProvider>
      <Header />
    </AppSettingProvider>
  </TimeProvider>
)

export {Header}
