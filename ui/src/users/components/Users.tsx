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
        Error: <pre>{JSON.stringify(error, null, 2)}</pre>
      </div>
    )
  }

  return <pre>{JSON.stringify(data, null, 2)}</pre>
}

export default UsersPage
