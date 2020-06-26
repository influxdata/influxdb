// Libraries
import React, {FC, useContext} from 'react'

// Components
import {
  Button,
  ComponentColor,
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'

// Constants
import {NotebookContext} from 'src/notebooks/context/notebook'
import {PIPE_DEFINITIONS} from 'src/notebooks'

import {event} from 'src/notebooks/shared/event'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

interface Props {
  index: number
  onInsert: () => void
}

const AddCellMenu: FC<Props> = ({index, onInsert}) => {
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

            onInsert()

            event('Notebook Add Button Clicked', {
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

  return (
    <FlexBox
      direction={FlexDirection.Column}
      alignItems={AlignItems.Stretch}
      margin={ComponentSize.Small}
      className="insert-cell-menu"
    >
      <p className="insert-cell-menu--title">Insert Cell Here</p>
      {pipes}
    </FlexBox>
  )
}

export default AddCellMenu
