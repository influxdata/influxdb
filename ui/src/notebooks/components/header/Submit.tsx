// Libraries
import React, {FC, useContext, useEffect} from 'react'
import {SubmitQueryButton} from 'src/timeMachine/components/SubmitQueryButton'
import QueryProvider, {QueryContext} from 'src/notebooks/context/query'
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'
import {TimeContext} from 'src/notebooks/context/time'
import {IconFont} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

const PREVIOUS_REGEXP = /__PREVIOUS_RESULT__/g

export const Submit: FC = () => {
  const {query} = useContext(QueryContext)
  const {id, pipes, updateResult, updateMeta} = useContext(NotebookContext)
  const {timeContext} = useContext(TimeContext)
  const time = timeContext[id]

  useEffect(() => {
    submit()
  }, [!!time && time.range])

  const submit = () => {
    pipes
      .reduce((stages, pipe, index) => {
        updateMeta(index, {loading: true} as PipeMeta)

        if (pipe.type === 'query') {
          let text = pipe.queries[pipe.activeQuery].text.replace(
            /(\/\*([\s\S]*?)\*\/)|(\/\/(.*)$)/gm,
            ''
          )
          let requirements = {}

          if (PREVIOUS_REGEXP.test(text)) {
            requirements = {
              ...(index === 0 ? {} : stages[stages.length - 1].requirements),
              [`prev_${index}`]: stages[stages.length - 1].text,
            }
            text = text.replace(PREVIOUS_REGEXP, `prev_${index}`)
          }

          stages.push({
            text,
            instances: [index],
            requirements,
          })
        } else {
          stages[stages.length - 1].instances.push(index)
        }

        return stages
      }, [])
      .map(queryStruct => {
        const queryText =
          Object.entries(queryStruct.requirements)
            .map(([key, value]) => `${key} = (\n${value}\n)\n\n`)
            .join('') + queryStruct.text

        return query(queryText).then(response => {
          queryStruct.instances.forEach(index => {
            updateMeta(index, {loading: false} as PipeMeta)
            updateResult(index, response)
          })
        })
      })
  }

  return (
    <SubmitQueryButton
      text="Run Notebook"
      icon={IconFont.Play}
      submitButtonDisabled={false}
      queryStatus={RemoteDataState.NotStarted}
      onSubmit={submit}
    />
  )
}

export default () => (
  <QueryProvider>
    <Submit />
  </QueryProvider>
)
