// Libraries
import React, {FC, useContext, useState, useEffect} from 'react'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'
import QueryProvider, {QueryContext} from 'src/notebooks/context/query'
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {ResultsContext} from 'src/notebooks/context/results'
import {TimeContext} from 'src/notebooks/context/time'
import {IconFont} from '@influxdata/clockface'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Types
import {RemoteDataState} from 'src/types'

const PREVIOUS_REGEXP = /__PREVIOUS_RESULT__/g
const COMMENT_REMOVER = /(\/\*([\s\S]*?)\*\/)|(\/\/(.*)$)/gm

const fakeNotify = notify

export const Submit: FC = () => {
  const {query} = useContext(QueryContext)
  const {id, notebook} = useContext(NotebookContext)
  const {add, update} = useContext(ResultsContext)
  const {timeContext} = useContext(TimeContext)
  const [isLoading, setLoading] = useState(RemoteDataState.NotStarted)
  const time = timeContext[id]
  const tr = !!time && time.range

  useEffect(() => {
    submit()
  }, [tr]) // eslint-disable-line react-hooks/exhaustive-deps

  const forceUpdate = (id, data) => {
    try {
      update(id, data)
    } catch (_e) {
      add(id, data)
    }
  }

  const submit = () => {
    event('Notebook Submit Button Clicked')
    let queryIncludesPreviousResult = false
    setLoading(RemoteDataState.Loading)
    Promise.all(
      notebook.data.allIDs
        .reduce((stages, pipeID, index) => {
          notebook.meta.update(pipeID, {loading: RemoteDataState.Loading})
          const pipe = notebook.data.get(pipeID)

          if (pipe.type === 'query') {
            let text = pipe.queries[pipe.activeQuery].text.replace(
              COMMENT_REMOVER,
              ''
            )
            let requirements = {}

            if (!text.replace(/\s/g, '').length) {
              if (stages.length) {
                stages[stages.length - 1].instances.push(pipeID)
              }
              return stages
            }

            if (PREVIOUS_REGEXP.test(text)) {
              requirements = {
                ...(index === 0 ? {} : stages[stages.length - 1].requirements),
                [`prev_${index}`]: stages[stages.length - 1].text,
              }
              text = text.replace(PREVIOUS_REGEXP, `prev_${index}`)
              queryIncludesPreviousResult = true
            }

            stages.push({
              text,
              instances: [pipeID],
              requirements,
            })
          } else if (pipe.type === 'data') {
            const {
              aggregateFunction,
              bucketName,
              field,
              measurement,
              tags,
            } = pipe

            let text = `from(bucket: "${bucketName}")|>range(start: v.timeRangeStart, stop: v.timeRangeStop)`
            if (measurement) {
              text += `|> filter(fn: (r) => r["_measurement"] == "${measurement}")`
            }
            if (field) {
              text += `|> filter(fn: (r) => r["_field"] == "${field}")`
            }
            if (tags && Object.keys(tags)?.length > 0) {
              Object.keys(tags)
                .filter((tagName: string) => !!tags[tagName])
                .forEach((tagName: string) => {
                  const tagValues = tags[tagName]
                  if (tagValues.length === 1) {
                    text += `|> filter(fn: (r) => r["${tagName}"] == "${tagValues[0]}")`
                  } else {
                    tagValues.forEach((val, i) => {
                      if (i === 0) {
                        text += `|> filter(fn: (r) => r["${tagName}"] == "${val}"`
                      }
                      if (tagValues.length - 1 === i) {
                        text += ` or r["${tagName}"] == "${val}")`
                      } else {
                        text += ` or r["${tagName}"] == "${val}"`
                      }
                    })
                  }
                })
            }

            if (aggregateFunction.flux && aggregateFunction.name) {
              text += `  |> aggregateWindow(every: v.windowPeriod, fn: ${aggregateFunction.name}, createEmpty: false)
              |> yield(name: "${aggregateFunction.name}")`
            }

            stages.push({
              text,
              instances: [pipeID],
              requirements: {},
            })
          } else if (stages.length) {
            stages[stages.length - 1].instances.push(pipeID)
          }

          return stages
        }, [])
        .map(queryStruct => {
          const queryText =
            Object.entries(queryStruct.requirements)
              .map(([key, value]) => `${key} = (\n${value}\n)\n\n`)
              .join('') + queryStruct.text

          return query(queryText)
            .then(response => {
              queryStruct.instances.forEach(pipeID => {
                forceUpdate(pipeID, response)
                notebook.meta.update(pipeID, {loading: RemoteDataState.Done})
              })
            })
            .catch(e => {
              queryStruct.instances.forEach(pipeID => {
                forceUpdate(pipeID, {
                  error: e.message,
                })
                notebook.meta.update(pipeID, {loading: RemoteDataState.Error})
              })
            })
        })
    )

      .then(() => {
        event('Notebook Submit Resolved')

        if (queryIncludesPreviousResult) {
          event('flows_queryIncludesPreviousResult')
        } else {
          event('flows_queryExcludesPreviousResult')
        }

        setLoading(RemoteDataState.Done)
      })
      .catch(e => {
        event('Notebook Submit Resolved')

        // NOTE: this shouldn't fire, but lets wrap it for completeness
        setLoading(RemoteDataState.Error)
        throw e
      })
  }

  const hasQueries = notebook.data.all
    .map(p => p.type)
    .filter(p => p === 'query' || p === 'data').length

  return (
    <SubmitQueryButton
      text="Run Flow"
      className="flows-run-flow"
      icon={IconFont.Play}
      submitButtonDisabled={!hasQueries}
      queryStatus={isLoading}
      onSubmit={submit}
      onNotify={fakeNotify}
    />
  )
}

export default () => (
  <QueryProvider>
    <Submit />
  </QueryProvider>
)
