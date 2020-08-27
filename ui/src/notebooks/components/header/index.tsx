// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider from 'src/notebooks/context/app'

// Components
import {Page} from '@influxdata/clockface'
import TimeZoneDropdown from 'src/notebooks/components/header/TimeZoneDropdown'
import TimeRangeDropdown from 'src/notebooks/components/header/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/notebooks/components/header/AutoRefreshDropdown'
import Submit from 'src/notebooks/components/header/Submit'
import PresentationMode from 'src/notebooks/components/header/PresentationMode'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'

const FULL_WIDTH = true

export interface TimeContextProps {
  context: TimeBlock
  update: (data: TimeBlock) => void
}

const NotebookHeader: FC = () => {
  const {id, update, notebook} = useContext(NotebookContext)
  const {timeContext, addTimeContext, updateTimeContext} = useContext(
    TimeContext
  )

  const updateTime = useCallback(
    (data: TimeBlock) => {
      updateTimeContext(id, data)
    },
    [id, updateTimeContext]
  )

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  const handleRename = (name: string) => {
    update({...notebook, name})
  }

  return (
    <>
      <Page.Header fullWidth={FULL_WIDTH}>
        <RenamablePageTitle
          onRename={handleRename}
          name={notebook.name}
          placeholder="Name this Flow"
          maxLength={50}
        />
      </Page.Header>
      <Page.ControlBar fullWidth={FULL_WIDTH}>
        <Page.ControlBarLeft>
          <Submit />
        </Page.ControlBarLeft>
        <Page.ControlBarRight>
          <PresentationMode />
          <TimeZoneDropdown />
          <TimeRangeDropdown context={timeContext[id]} update={updateTime} />
          <AutoRefreshDropdown context={timeContext[id]} update={updateTime} />
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
