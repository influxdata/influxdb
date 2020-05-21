import React, {FC} from 'react'

import {getOrgsUsers} from 'src/client/unifyRoutes'
import useClient from 'src/usage/components/useClient'

import {RemoteDataState} from 'src/types'

const UsersPage: FC = () => {
  const getUsers = () => getOrgsUsers({orgID: '0000000000000001'})
  const [status, data, error] = useClient(getUsers)

  if (
    status === RemoteDataState.Loading ||
    status === RemoteDataState.NotStarted
  ) {
    return <div>Loading...</div>
  }

  if (status === RemoteDataState.Error) {
    return (
      <div>
        Error: <code>{JSON.stringify(error, null, 2)}</code>
      </div>
    )
  }

  return <code>{JSON.stringify(data, null, 2)}</code>
}

export default UsersPage
