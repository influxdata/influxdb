// Libraries
import React, {FC, useReducer, Dispatch} from 'react'
import {
  Page,
  Tabs,
  Orientation,
  ComponentSize,
  RemoteDataState,
} from '@influxdata/clockface'
import {CloudUser, Invite} from 'src/types'

// Components
import UserList from './UserList'
import UserListInviteForm from './UserListInviteForm'
import UsersPageHeader from './UsersPageHeader'

import {
  UserListState,
  Action,
  userListReducer,
  UserListReducer,
  initialState,
} from '../reducers'

export const UserListContext = React.createContext(null)
export type UserListContextResult = [UserListState, Dispatch<Action>]

interface Props {
  currentUserID?: string
  orgID?: string
  users: CloudUser[]
  invites: Invite[]
}

const UserListContainer: FC<Props> = ({
  currentUserID = '0000000000000001',
  users,
  invites,
  orgID = '0000000000000001',
}) => {
  const [state, dispatch] = useReducer<UserListReducer>(
    userListReducer,
    initialState({
      currentUserID,
      users: users.map(user => ({...user, status: RemoteDataState.Done})),
      invites: invites.map(invite => ({
        ...invite,
        status: RemoteDataState.Done,
      })),
      orgID,
    })
  )

  return (
    <Page titleTag="Users">
      <UsersPageHeader />
      <Page.Contents scrollable={true}>
        <Tabs.Container orientation={Orientation.Horizontal}>
          <Tabs size={ComponentSize.Large}>
            <Tabs.Tab
              id="users"
              text="Users"
              active={true}
              linkElement={className => (
                <a className={className} href={`/orgs/${orgID}/users`} />
              )}
            />
            <Tabs.Tab
              id="about"
              text="About"
              active={false}
              linkElement={className => (
                <a className={className} href="/about" />
              )}
            />
          </Tabs>
          <Tabs.TabContents>
            <UserListContext.Provider value={[state, dispatch]}>
              <UserListInviteForm />
              <UserList />
            </UserListContext.Provider>
          </Tabs.TabContents>
        </Tabs.Container>
      </Page.Contents>
    </Page>
  )
}

export default UserListContainer
