// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {
  PermissionsWidgetMode,
  PermissionsWidgetSelection,
} from 'src/shared/components/permissionsWidget/PermissionsWidget'
import {IconFont} from 'src/clockface'

interface Props {
  mode?: PermissionsWidgetMode
  id: string
  testID?: string
  label: string
  selected: PermissionsWidgetSelection
  onToggle?: (
    permissionID: string,
    newState: PermissionsWidgetSelection
  ) => void
}

class PermissionsWidgetItem extends Component<Props> {
  public render() {
    const {label, testID} = this.props

    return (
      <li
        className={this.className}
        onClick={this.handleClick}
        data-testid={testID || 'permissions--item'}
      >
        {this.checkbox}
        <label className="permissions-widget--item-label">{label}</label>
      </li>
    )
  }

  private get checkbox(): JSX.Element {
    const {mode, selected} = this.props

    if (mode === PermissionsWidgetMode.Write) {
      return (
        <div className="permissions-widget--checkbox">
          <span className={`icon ${IconFont.Checkmark}`} />
        </div>
      )
    }

    if (selected === PermissionsWidgetSelection.Selected) {
      return (
        <div className="permissions-widget--icon">
          <span className={`icon ${IconFont.Checkmark}`} />
        </div>
      )
    }

    return (
      <div className="permissions-widget--icon">
        <span className={`icon ${IconFont.Remove}`} />
      </div>
    )
  }

  private get className(): string {
    const {selected, mode} = this.props

    return classnames('permissions-widget--item', {
      selected: selected === PermissionsWidgetSelection.Selected,
      unselected: selected === PermissionsWidgetSelection.Unselected,
      selectable: mode === PermissionsWidgetMode.Write,
    })
  }

  private handleClick = (): void => {
    const {id, mode, selected, onToggle} = this.props

    if (mode === PermissionsWidgetMode.Read || !onToggle) {
      return
    }

    if (selected === PermissionsWidgetSelection.Selected) {
      onToggle(id, PermissionsWidgetSelection.Unselected)
    } else if (selected === PermissionsWidgetSelection.Unselected) {
      onToggle(id, PermissionsWidgetSelection.Selected)
    }
  }
}

export default PermissionsWidgetItem
