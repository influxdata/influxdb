import React, {FC, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/notebook.context'
import {TimeProvider, TimeContext} from 'src/notebooks/time.context'
import AppSettingProvider, {AppSettingContext} from 'src/notebooks/app.context'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import {AutoRefreshStatus} from 'src/types'
import {TimeZoneDropdown} from 'src/shared/components/TimeZoneDropdown'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'

const PREVIOUS_REGEXP = /__PREVIOUS_RESULT__/g
const NotebookHeader: FC = () => {
  const { id, pipes } = useContext(NotebookContext)
  const { timeContext, addTimeContext, updateTimeContext } = useContext(TimeContext)
  const { timeZone, onSetTimeZone } = useContext(AppSettingContext)

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

  function submit() {
      const queries = pipes.reduce((stages, pipe, idx) => {
          if (pipe.type === 'query') {
              let text = pipe.queries[pipe.activeQuery].text
              let requirements = {}

              if (PREVIOUS_REGEXP.test(text)) {
                  requirements = {
                      ...(idx === 0 ? {} : stages[stages.length - 1].requirements),
                      [`prev_${idx}`]: stages[stages.length - 1].text
                  }
                  text = text.replace(PREVIOUS_REGEXP, `prev_${idx}`)
              }

              stages.push({
                  text,
                  instances: [ idx ],
                  requirements
              })
          } else {
              stages[stages.length - 1].instances.push(idx);
          }

          return stages
      }, [])

      queries.forEach(q => {
          console.log('FIRING')
          console.log(Object.entries(q.requirements).map(([key, value]) => `${key} = (\n${value}\n)\n\n`).join('') + q.text)
          console.log('for', q.instances)
      })
  }

  return (
    <>
      <h1>NOTEBOOKS</h1>
        <div className="notebook-header--buttons">
            <TimeZoneDropdown
                timeZone={ timeZone }
                onSetTimeZone={ onSetTimeZone } />
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
                onSubmit={ submit } />
        </div>
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
