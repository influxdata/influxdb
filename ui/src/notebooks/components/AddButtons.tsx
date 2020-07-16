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

import {TypeRegistration} from 'src/notebooks'

// Styles
import 'src/notebooks/components/AddButtons.scss'

interface Props {
  index?: number
  onInsert?: () => void
  eventName: string
}

const SUPPORTED_FAMILIES = [
  {
    name: 'Input',
    family: 'inputs',
  },
  {
    name: 'Transform',
    family: 'transform',
  },
  {
    name: 'Pass Throughs',
    family: 'passThrough',
  },
  {
    name: 'Test',
    family: 'test',
  },
  {
    name: 'Output',
    family: 'output',
  },
  {
    name: 'Side Effects',
    family: 'sideEffects',
  },
]

const AddButtons: FC<Props> = ({index, onInsert, eventName}) => {
  const {add} = useContext(NotebookContext)
  const results = useContext(ResultsContext)

  const pipeFamilies = Object.entries(
    Object.values(PIPE_DEFINITIONS)
      .filter(
        def =>
          !def.disabled && (!def.featureFlag || isFlagEnabled(def.featureFlag))
      )
      .reduce((acc, def) => {
        if (!acc.hasOwnProperty(def.family)) {
          acc[def.family] = []
        }

        acc[def.family].push(def)

        return acc
      }, {})
  ).reduce((acc, [key, val]) => {
    acc[key] = (val as TypeRegistration[]).sort((a, b) => {
      const aPriority = a.priority || 0
      const bPriority = b.priority || 0

      if (aPriority === bPriority) {
        return a.button.localeCompare(b.button)
      }

      return bPriority - aPriority
    })
    return acc
  }, {})

  const cellFamilies = SUPPORTED_FAMILIES.filter(fam =>
    pipeFamilies.hasOwnProperty(fam.family)
  ).map(fam => {
    const pipes = pipeFamilies[fam.family].map(def => {
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
                type: def.type,
              },
              index
            )

            results.add(id)
          }}
          color={ComponentColor.Secondary}
        />
      )
    })
    return <CellFamily title={fam.name}>{pipes}</CellFamily>
  })

  return <div className="add-cell-menu">{cellFamilies}</div>
}

export default AddButtons
