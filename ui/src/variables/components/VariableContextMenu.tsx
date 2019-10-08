// Libraries
import React, {PureComponent} from 'react'

// Components
import {Context} from 'src/clockface'
import {IconFont, ComponentColor} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Types
import {Variable} from '@influxdata/influx'

interface Props {
  variable: Variable
  onDelete: (variable: Variable) => void
}

export default class VariableContextMenu extends PureComponent<Props> {
  public render() {
    const {variable, onDelete} = this.props

    return (
      <Context>
        <Context.Menu icon={IconFont.CogThick}>
          <OverlayLink overlayID="export-variable" resourceID={variable.id}>
            {onClick => <Context.Item label="Export" action={onClick} />}
          </OverlayLink>
          <OverlayLink overlayID="rename-variable" resourceID={variable.id}>
            {onClick => <Context.Item label="Rename" action={onClick} />}
          </OverlayLink>
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
            testID="context-delete-task"
          />
        </Context.Menu>
      </Context>
    )
  }
}
