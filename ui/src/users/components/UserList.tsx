// Libraries
import React, {Dispatch, FC, useContext, useState} from 'react'

// Components
import {Columns, Grid, IconFont, IndexList, Input} from '@influxdata/clockface'
import {UserListContext} from './UserListContainer'
import UserListItem from './UserListItem'
import InviteListItem from './InviteListItem'
import UserListNotifications from './UserListNotifications'

// Actions
import {Action, UserListState} from '../reducers'

// Utils
import {filter} from 'src/users/utils'

import {CloudUser} from 'src/types'

const UserList: FC = () => {
  const [{users, invites}] = useContext<[UserListState, Dispatch<Action>]>(
    UserListContext
  )

  const [searchTerm, setSearchTerm] = useState('')

  const filteredUsers = filter<CloudUser, 'email' | 'role'>(
    users,
    ['email', 'role'],
    searchTerm
  )

  const filteredInvites = filter(invites, ['email', 'role'], searchTerm)

  return (
    <Grid>
      <Grid.Row>
        <Grid.Column widthMD={Columns.Ten} widthLG={Columns.Six}>
          <Input
            icon={IconFont.Search}
            placeholder="Filter members..."
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
          />
        </Grid.Column>
      </Grid.Row>
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell width="30%" columnName="email" />
          {/* TODO: add back in once https://github.com/influxdata/quartz/issues/2389 back-filling of names is complete */}
          {/*<IndexList.HeaderCell width="30%" columnName="name" />*/}
          <IndexList.HeaderCell width="10%" columnName="role" />
          <IndexList.HeaderCell width="20%" columnName="status" />
          <IndexList.HeaderCell width="10%" columnName="" />
        </IndexList.Header>
        <IndexList.Body emptyState={null} columnCount={4}>
          {filteredInvites.map(invite => (
            <InviteListItem key={`invite-${invite.id}`} invite={invite} />
          ))}
          {filteredUsers.map(user => (
            <UserListItem key={`user-${user.id}`} user={user} />
          ))}
        </IndexList.Body>
      </IndexList>
      <UserListNotifications />
    </Grid>
  )
}

export default UserList
