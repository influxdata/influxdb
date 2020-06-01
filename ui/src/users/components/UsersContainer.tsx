import React, {FC} from 'react'
import Users from './Users'
import Invites from './Invites'

const UsersContainer: FC = () => {
  return (
    <>
      <Users />
      <Invites />
    </>
  )
}

export default UsersContainer
