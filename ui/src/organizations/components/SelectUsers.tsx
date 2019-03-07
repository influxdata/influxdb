// Libraries
import React, {PureComponent} from 'react'

//Components
import {MultiSelectDropdown, Dropdown} from 'src/clockface'

// Types
import {UsersMap} from 'src/organizations/components/Members'

interface Props {
  users: UsersMap
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
          {Object.keys(users).map(key => (
            <Dropdown.Item id={key} key={key} value={users[key]}>
              {users[key].name}
            </Dropdown.Item>
          ))}
        </MultiSelectDropdown>
      </>
    )
  }
}
