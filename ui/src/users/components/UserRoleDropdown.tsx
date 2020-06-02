// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Dropdown, ComponentColor} from '@influxdata/clockface'
import {UserListContext} from './UserListContainer'

// Constants
import {roles} from '../constants'

// Types
import {Role} from 'src/types'

// Actions
import {editDraftInvite} from '../reducers'

const UserRoleDropdown: FC = () => {
  const [{draftInvite}, dispatch] = useContext(UserListContext)

  const onChangeRole = (role: Role) => () => {
    dispatch(editDraftInvite({...draftInvite, role}))
  }

  const dropdownButton = (active, onClick) => (
    <Dropdown.Button
      className="user-role--dropdown--button"
      active={active}
      onClick={onClick}
      color={ComponentColor.Primary}
    >
      {draftInvite.role}
    </Dropdown.Button>
  )

  const dropdownItems = roles.map(role => (
    <Dropdown.Item
      className="user-role--dropdown-item"
      id={role}
      key={role}
      value={role}
      onClick={onChangeRole(role)}
    >
      {role}
    </Dropdown.Item>
  ))

  const dropdownMenu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{dropdownItems}</Dropdown.Menu>
  )

  return <Dropdown button={dropdownButton} menu={dropdownMenu} />
}

export default UserRoleDropdown
