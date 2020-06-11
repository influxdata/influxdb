// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Context, IconFont} from 'src/clockface'
import {ComponentColor} from '@influxdata/clockface'

interface Props {
  onView: () => void
  onDelete: () => void
  onClone: () => void
}

const CheckCardContext: FunctionComponent<Props> = ({
  onDelete,
  onClone,
  onView,
}) => {
  return (
    <Context>
      <Context.Menu icon={IconFont.EyeOpen} testID="context-history-menu">
        <Context.Item
          label="View History"
          action={onView}
          testID="context-history-task"
        />
      </Context.Menu>
      <Context.Menu icon={IconFont.Duplicate} color={ComponentColor.Secondary}>
        <Context.Item label="Clone" action={onClone} />
      </Context.Menu>
      <Context.Menu
        icon={IconFont.Trash}
        color={ComponentColor.Danger}
        testID="context-delete-menu"
      >
        <Context.Item
          label="Delete"
          action={onDelete}
          testID="context-delete-task"
        />
      </Context.Menu>
    </Context>
  )
}

export default CheckCardContext
