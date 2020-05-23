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
import {parseFiles} from 'src/timeMachine/utils/rawFluxDataTable'
import {runQuery} from 'src/shared/apis/query'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {asAssignment} from 'src/variables/selectors'

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

const PREVIOUS_REGEXP = /__PREVIOUS_RESULT__/g
const ConnectedSubmitButton = React.memo(() => {
  const {timeZone, onSetTimeZone} = useContext(AppSettingContext)
  const {pipes, setResults} = useContext(NotebookContext)
  const {org, variables} = useContext(AppSettingContext)

  // TODO: move this to a central controller so that
  // pipes can request for themselves
  const submit = () => {
      const vars = variables.map(v => asAssignment(v))
        Promise.all(pipes.reduce((stages, pipe, idx) => {
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
        }, []).map(query => {
            const queryText = Object.entries(query.requirements).map(([key, value]) => `${key} = (\n${value}\n)\n\n`).join('') + query.text

        const windowVars = getWindowVars(queryText, vars)
        const extern = buildVarsOption([...vars, ...windowVars])

        return runQuery(org.id, queryText, extern).promise
            .then(raw => {
                if (raw.type !== 'SUCCESS') {
                    throw new Error('dun fucked up')
                }

                return raw
            })
            .then(raw => {
                return parseFiles([raw.csv])
            })
            .then(response => {
                return {
                    ...query,
                    results: response
                }
            })
        })).then(responses => {
            return responses.reduce((acc, { results, instances }) => {
                instances.map(index => {
                    acc[index] = results
                })

                return acc
            }, [])
        }).then(results => {
            setResults(results)
        })
  }

  return (
      <SubmitQueryButton
              submitButtonDisabled={false}
              queryStatus={RemoteDataState.NotStarted}
              onSubmit={submit}
            />
  )
})

const EnsureTimeContextExists: FC = () => {
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
              <ConnectedSubmitButton />
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
