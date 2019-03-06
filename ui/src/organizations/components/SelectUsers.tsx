// Libraries
import React, {PureComponent} from 'react'
import {MultiSelectDropdown, Dropdown} from 'src/clockface'
import {User} from '@influxdata/influx'

interface Props {
  users: User[]
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
}

export default class SelectUsers extends PureComponent<Props> {
  public render() {
    return (
      <>
        <MultiSelectDropdown
          selectedIDs={this.props.selectedUserIDs}
          onChange={this.props.onSelect}
          emptyText="Select user"
        >
          {this.props.users.map(cn => (
            <Dropdown.Item id={cn.id} key={cn.id} value={cn}>
              {cn.name}
            </Dropdown.Item>
          ))}
        </MultiSelectDropdown>
      </>
    )
  }
}
