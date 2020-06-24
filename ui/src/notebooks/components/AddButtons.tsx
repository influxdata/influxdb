// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Button, ComponentColor} from '@influxdata/clockface'

// Constants
import {NotebookContext} from 'src/notebooks/context/notebook'
import {PIPE_DEFINITIONS} from 'src/notebooks'

import {event} from 'src/cloud/utils/reporting'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

interface Props {
  index?: number
  onInsert?: () => void
  eventName: string
}

const AddButtons: FC<Props> = ({index, onInsert, eventName}) => {
  const {addPipe} = useContext(NotebookContext)

  const pipes = Object.entries(PIPE_DEFINITIONS)
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
    .map(([type, def]) => {
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

            addPipe(
              {
                ...data,
                type,
              },
              index
            )
          }}
          color={ComponentColor.Secondary}
        />
      )
    })

  return <>{pipes}</>
}

export default AddButtons
