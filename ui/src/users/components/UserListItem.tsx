// Libraries
import React, {FC, useContext, useState} from 'react'
import {capitalize} from 'lodash'

// Components
import {
  IconFont,
  IndexList,
  Alignment,
  ButtonShape,
  ComponentColor,
  ConfirmationButton,
  RemoteDataState,
  ComponentStatus,
} from '@influxdata/clockface'
import {UserListContext, UserListContextResult} from './UserListContainer'

// Thunks
import {removeUser} from '../thunks'

// Types
import {CloudUser} from 'src/types'

interface Props {
  user: CloudUser
}

// TODO: add back in once https://github.com/influxdata/quartz/issues/2389 back-filling of names is complete

// const formatName = (firstName: string | null, lastName: string | null) => {
//   if (firstName && lastName) {
//     return `${firstName} ${lastName}`
//   }
//
//   if (firstName) {
//     return firstName
//   }
//
//   if (lastName) {
//     return lastName
//   }
//
//   return ''
// }

const UserListItem: FC<Props> = ({user}) => {
  const {id, email, role} = user
  const [{orgID, currentUserID}, dispatch] = useContext<UserListContextResult>(
    UserListContext
  )

  const isCurrentUser = id === currentUserID
  const [revealOnHover, toggleRevealOnHover] = useState(true)

  const handleShow = () => {
    toggleRevealOnHover(false)
  }

  const handleHide = () => {
    toggleRevealOnHover(true)
  }

  const handleRemove = async () => {
    removeUser(dispatch, user, orgID)
  }

  const status =
    user.status === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
    <IndexList.Row brighten={true}>
      <IndexList.Cell>
        <span className="user-list-email">{email}</span>
      </IndexList.Cell>
      {/* TODO: add back in once https://github.com/influxdata/quartz/issues/2389 back-filling of names is complete */}
      {/*<IndexList.Cell>
        <span className="user-list-name">
          {formatName(firstName, lastName)}
        </span>
      </IndexList.Cell>*/}
      <IndexList.Cell className="user-list-cell-role">
        {capitalize(role)}
      </IndexList.Cell>
      <IndexList.Cell className="user-list-cell-status">Active</IndexList.Cell>
      <IndexList.Cell revealOnHover={revealOnHover} alignment={Alignment.Right}>
        {!isCurrentUser && (
          <ConfirmationButton
            icon={IconFont.Trash}
            onShow={handleShow}
            status={status}
            onHide={handleHide}
            confirmationLabel="This action will remove this user from accessing this organization"
            confirmationButtonText="Remove user access"
            titleText="Remove user access"
            confirmationButtonColor={ComponentColor.Danger}
            color={ComponentColor.Danger}
            shape={ButtonShape.Square}
            onConfirm={handleRemove}
          />
        )}
      </IndexList.Cell>
    </IndexList.Row>
  )
}

export default UserListItem
