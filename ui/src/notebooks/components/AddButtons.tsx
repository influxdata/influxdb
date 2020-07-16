// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Button, ComponentColor} from '@influxdata/clockface'
import CellFamily from 'src/notebooks/components/CellFamily'

// Constants
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {ResultsContext} from 'src/notebooks/context/results'
import {PIPE_DEFINITIONS} from 'src/notebooks'

// Utils
import {event} from 'src/cloud/utils/reporting'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Styles
import 'src/notebooks/components/AddButtons.scss'

interface Props {
  index?: number
  onInsert?: () => void
  eventName: string
}

const AddButtons: FC<Props> = ({index, onInsert, eventName}) => {
  const {add} = useContext(NotebookContext)
  const results = useContext(ResultsContext)

  const sortedPipes = Object.entries(PIPE_DEFINITIONS)
    .filter(
      ([_, def]) =>
        !def.disabled && (!def.featureFlag || isFlagEnabled(def.featureFlag))
    )
    .sort((a, b) => {
      const aPriority = a[1].priority || 0
      const bPriority = b[1].priority || 0

      if (aPriority === bPriority) {
        return a[1].button.localeCompare(b[1].button)
      }

      return bPriority - aPriority
    })

  const inputPipes = sortedPipes.filter(pipe => pipe[1].family === 'inputs')
  const passThroughPipes = sortedPipes.filter(
    pipe => pipe[1].family === 'passThrough'
  )
  const testPipes = sortedPipes.filter(pipe => pipe[1].family === 'test')

  const mapPipes = (pipes: any): JSX.Element[] => {
    return pipes.map(([type, def]) => {
      return (
        <Button
          key={def.type}
          text={def.button}
          onClick={() => {
            let data = def.initial
            if (typeof data === 'function') {
              data = data()
            }

            onInsert && onInsert()

            event(eventName, {
              type: def.type,
            })

            const id = add(
              {
                ...data,
                type,
              },
              index
            )

            results.add(id)
          }}
          color={ComponentColor.Secondary}
        />
      )
    })
  }

  return (
    <div className="add-cell-menu">
      <CellFamily title="Inputs">{mapPipes(inputPipes)}</CellFamily>
      <CellFamily title="Pass Through">{mapPipes(passThroughPipes)}</CellFamily>
      <CellFamily title="Test">{mapPipes(testPipes)}</CellFamily>
    </div>
  )
}

export default AddButtons
