import React, {FC, useContext} from 'react'
import {NotebookProvider, NotebookContext} from 'src/notebooks/notebook.context'
import {TimeProvider, TimeContext} from 'src/notebooks/time.context'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {AutoRefreshStatus} from 'src/types'
import {TimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'

const NotebookHeader: FC = () => {
  const { id } = useContext(NotebookContext)
  const { timeContext, addTimeContext, updateTimeContext } = useContext(TimeContext)

  if (!timeContext.hasOwnProperty(id)) {
      addTimeContext(id)
      return null
  }

  const {refresh, range} = timeContext[id]

  function updateRefresh(interval: number) {
      if (interval === 0) {
          updateTimeContext(id, {
              ...timeContext[id],
              refresh: {
                  status: AutoRefreshStatus.Paused,
                  interval
              }
          })
      } else {
          updateTimeContext(id, {
              ...timeContext[id],
              refresh: {
                  status: AutoRefreshStatus.Active,
                  interval
              }
          })
      }

  }

  function updateRange(range) {
      updateTimeContext(id, {
          ...timeContext[id],
          range
      })
  }

  return (
    <>
      <h1>NOTEBOOKS</h1>
        <div className="notebook-header--buttons">
            <TimeZoneDropdown
                timeZone={ 'Local' }
                onSetTimeZone={ ()=>{} } />
            <TimeRangeDropdown
                timeRange={range}
                onSetTimeRange={ updateRange } />
            <AutoRefreshDropdown
                selected={refresh}
                onChoose={ updateRefresh }
                showManualRefresh={ false } />
            <SubmitQueryButton
                submitButtonDisabled={ false }
                queryStatus={ "NotStarted" }
                onSubmit={ ()=> {} } />
        </div>
    </>
  )
}


export default () => (
    <NotebookProvider>
        <TimeProvider>
            <NotebookHeader />
        </TimeProvider>
    </NotebookProvider>
)
