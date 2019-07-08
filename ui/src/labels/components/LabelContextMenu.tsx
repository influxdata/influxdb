// Libraries
import React, {PureComponent} from 'react'

// Components
import {Context} from 'src/clockface'
import {IconFont, ComponentColor} from '@influxdata/clockface'

// Types
import {Label} from 'src/types'

interface Props {
  label: Label
  onDelete: (labelID: string) => void
}

export default class LabelContextMenu extends PureComponent<Props> {
  public render() {
    const {label, onDelete} = this.props

    return (
      <Context>
        <Context.Menu
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
          testID="context-delete-menu"
        >
          <Context.Item
            label="Delete"
            action={onDelete}
            value={label.id}
            testID="context-delete-label"
          />
        </Context.Menu>
      </Context>
    )
  }
}
