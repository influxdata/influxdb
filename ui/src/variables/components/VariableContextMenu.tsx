// Libraries
import React, {PureComponent} from 'react'

// Components
import {Context} from 'src/clockface'
import {IconFont, ComponentColor} from '@influxdata/clockface'

// Types
import {Variable} from 'src/types'

interface Props {
  variable: Variable
  onExport: () => void
  onRename: () => void
  onDelete: (variable: Variable) => void
}

export default class VariableContextMenu extends PureComponent<Props> {
  public render() {
    const {variable, onExport, onRename, onDelete} = this.props

    return (
      <Context>
        <Context.Menu icon={IconFont.CogThick}>
          <Context.Item label="Export" action={onExport} />
          <Context.Item
            label="Rename"
            action={onRename}
            testID="context-rename-variable"
          />
        </Context.Menu>
        <Context.Menu
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
          testID="context-delete-menu"
        >
          <Context.Item
            label="Delete"
            action={onDelete}
            value={variable}
            testID="context-delete-variable"
          />
        </Context.Menu>
      </Context>
    )
  }
}
