// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider from 'src/notebooks/context/app'

// Components
import {Page} from '@influxdata/clockface'
import TimeZoneDropdown from 'src/notebooks/components/header/TimeZoneDropdown'
import TimeRangeDropdown from 'src/notebooks/components/header/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/notebooks/components/header/AutoRefreshDropdown'
import Submit from 'src/notebooks/components/header/Submit'
import PresentationMode from 'src/notebooks/components/header/PresentationMode'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

const FULL_WIDTH = true

export interface TimeContextProps {
  context: TimeBlock
  update: (data: TimeBlock) => void
}

const NotebookHeader: FC = () => {
  const {id} = useContext(NotebookContext)
  const {timeContext, addTimeContext, updateTimeContext} = useContext(
    TimeContext
  )

  const update = useCallback(
    (data: TimeBlock) => {
      updateTimeContext(id, data)
    },
    [id, updateTimeContext]
  )

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  return (
    <>
      <Page.Header fullWidth={FULL_WIDTH}>
        <Page.Title title="Flows" />
      </Page.Header>
      <Page.ControlBar fullWidth={FULL_WIDTH}>
        <Page.ControlBarLeft>
          <Submit />
        </Page.ControlBarLeft>
        <Page.ControlBarRight>
          <PresentationMode />
          <TimeZoneDropdown />
          <FeatureFlag name="notebook-panel--data-source" equals={false}>
            <TimeRangeDropdown context={timeContext[id]} update={update} />
            <AutoRefreshDropdown context={timeContext[id]} update={update} />
          </FeatureFlag>
        </Page.ControlBarRight>
      </Page.ControlBar>
    </>
  )
}

export default () => (
  <TimeProvider>
    <AppSettingProvider>
      <NotebookHeader />
    </AppSettingProvider>
  </TimeProvider>
)
