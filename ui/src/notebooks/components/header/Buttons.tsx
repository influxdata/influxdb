// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider from 'src/notebooks/context/app'

// Components
import TimeZoneDropdown from 'src/notebooks/components/header/TimeZoneDropdown'
import TimeRangeDropdown from 'src/notebooks/components/header/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/notebooks/components/header/AutoRefreshDropdown'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'
import {IconFont} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

export interface TimeContextProps {
  context: TimeBlock
  update: (data: TimeBlock) => void
}

const Buttons: FC = () => {
  const {id} = useContext(NotebookContext)
  const {timeContext, addTimeContext, updateTimeContext} = useContext(
    TimeContext
  )

  const update = useCallback(
    (data: TimeBlock) => {
      updateTimeContext(id, data)
    },
    [id]
  )

  function submit() {} // eslint-disable-line @typescript-eslint/no-empty-function

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  return (
    <TimeProvider>
      <AppSettingProvider>
        <div className="notebook-header--buttons">
          <TimeZoneDropdown />
          <TimeRangeDropdown context={timeContext[id]} update={update} />
          <AutoRefreshDropdown context={timeContext[id]} update={update} />
          <SubmitQueryButton
            text="Run Notebook"
            icon={IconFont.Play}
            submitButtonDisabled={false}
            queryStatus={RemoteDataState.NotStarted}
            onSubmit={submit}
          />
        </div>
      </AppSettingProvider>
    </TimeProvider>
  )
}

export default Buttons
