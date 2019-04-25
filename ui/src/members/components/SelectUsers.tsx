// Libraries
import React, {PureComponent} from 'react'

//Components
import {MultiSelectDropdown, Dropdown} from 'src/clockface'

// Types
import {User} from '@influxdata/influx'
import {ComponentStatus} from '@influxdata/clockface'

interface Props {
  users: User[]
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
}

export default class SelectUsers extends PureComponent<Props> {
  public render() {
    const {users, selectedUserIDs, onSelect} = this.props

    return (
      <MultiSelectDropdown
        selectedIDs={selectedUserIDs}
        onChange={onSelect}
        emptyText={this.emptyText}
        status={this.dropdownStatus}
      >
        {users.map(u => (
          <Dropdown.Item id={u.id} key={u.id} value={u}>
            {u.name}
          </Dropdown.Item>
        ))}
      </MultiSelectDropdown>
    )
  }

  private get emptyText(): string {
    const {users} = this.props
    if (!users || !users.length) {
      return 'No users exist'
    }
    return 'Select user'
  }

  private get dropdownStatus(): ComponentStatus {
    const {users} = this.props
    if (!users || !users.length) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}
