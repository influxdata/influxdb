// Libraries
import React, {PureComponent} from 'react'

//Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {User} from '@influxdata/influx'
import {ComponentStatus, DropdownItemType} from '@influxdata/clockface'

interface Props {
  users: User[]
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
}

export default class SelectUsers extends PureComponent<Props> {
  public render() {
    const {users} = this.props

    return (
      <Dropdown
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            status={this.dropdownStatus}
          >
            {this.dropdownLabel}
          </Dropdown.Button>
        )}
        menu={() => (
          <Dropdown.Menu>
            {users.map(u => (
              <Dropdown.Item
                type={DropdownItemType.Checkbox}
                id={u.id}
                key={u.id}
                value={u}
                selected={this.highlightSelectedItem(u)}
                onClick={this.handleItemClick}
              >
                {u.name}
              </Dropdown.Item>
            ))}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private handleItemClick = (user: User): void => {
    const {selectedUserIDs, onSelect} = this.props

    let updatedSelectedUserIDs

    if (selectedUserIDs.includes(user.id)) {
      updatedSelectedUserIDs = selectedUserIDs.filter(id => id !== user.id)
    } else {
      updatedSelectedUserIDs = [...selectedUserIDs, user.id]
    }

    return onSelect(updatedSelectedUserIDs)
  }

  private highlightSelectedItem = (user: User): boolean => {
    const {selectedUserIDs} = this.props

    if (selectedUserIDs.includes(user.id)) {
      return true
    }

    return false
  }

  private get dropdownLabel(): string {
    const {users, selectedUserIDs} = this.props

    if (!users || !users.length) {
      return 'No users exist'
    }

    if (!selectedUserIDs.length) {
      return 'Select users'
    }

    const userNames = users
      .filter(user => selectedUserIDs.includes(user.id))
      .map(user => user.name)

    return userNames.join(', ')
  }

  private get dropdownStatus(): ComponentStatus {
    const {users} = this.props
    if (!users || !users.length) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}
