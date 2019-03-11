// Libraries
import React, {PureComponent} from 'react'

//Components
import {MultiSelectDropdown, Dropdown} from 'src/clockface'

// Types
import {User} from '@influxdata/influx'

interface Props {
  users: User[]
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
}

export default class SelectUsers extends PureComponent<Props> {
  public render() {
    const {users} = this.props

    return (
      <>
        <MultiSelectDropdown
          selectedIDs={this.props.selectedUserIDs}
          onChange={this.props.onSelect}
          emptyText="Select user"
        >
          {users.map(u => (
            <Dropdown.Item id={u.id} key={u.id} value={u}>
              {u.name}
            </Dropdown.Item>
          ))}
        </MultiSelectDropdown>
      </>
    )
  }
}
