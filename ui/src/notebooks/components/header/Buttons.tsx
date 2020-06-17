// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeProvider, TimeContext, TimeBlock} from 'src/notebooks/context/time'
import AppSettingProvider from 'src/notebooks/context/app'

// Components
import AutoRefreshDropdown from 'src/notebooks/components/header/AutoRefreshDropdown'
import Submit from 'src/notebooks/components/header/Submit'

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

  if (!timeContext.hasOwnProperty(id)) {
    addTimeContext(id)
    return null
  }

  return (
    <div className="notebook-header--buttons">
      <AutoRefreshDropdown context={timeContext[id]} update={update} />
      <Submit />
    </div>
  )
}

export default () => (
  <TimeProvider>
    <AppSettingProvider>
      <Buttons />
    </AppSettingProvider>
  </TimeProvider>
)
