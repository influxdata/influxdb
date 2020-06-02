import React, {FC} from 'react'

import {getOrgsUsers, getOrgsInvites} from 'src/client/unifyRoutes'

import useClient from 'src/usage/components/useClient'

import {RemoteDataState} from 'src/types'
import UserListContainer from './UserListContainer'

const UsersPage: FC = () => {
  const params = {orgID: '0000000000000001'}
  const [status, data, error] = useClient([
    getOrgsUsers(params),
    getOrgsInvites(params),
  ])

  if (
    status === RemoteDataState.Loading ||
    status === RemoteDataState.NotStarted
  ) {
    return <div>Loading...</div>
  }

  if (status === RemoteDataState.Error) {
    return (
      <div>
        Error: <pre>{JSON.stringify(error, null, 2)}</pre>
      </div>
    )
  }

  if (status === RemoteDataState.Done && data) {
    const [{users}, {invites}] = data
    return <UserListContainer users={users} invites={invites} />
  }

  return null
}

export default UsersPage
