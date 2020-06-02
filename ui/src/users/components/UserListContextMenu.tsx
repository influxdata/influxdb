import React, {FC, useState, useContext} from 'react'

import {
  RemoteDataState,
  IndexList,
  Alignment,
  IconFont,
  ComponentColor,
  ButtonShape,
  ConfirmationButton,
  ComponentStatus,
  TechnoSpinner,
  ComponentSize,
} from '@influxdata/clockface'

import {UserListContext, UserListContextResult} from './UserListContainer'

// Thunks
import {removeUser} from '../thunks'

// Types
import {CloudUser} from 'src/types'

interface Props {
  user: CloudUser
}

const UserListContextMenu: FC<Props> = ({user}) => {
  const [context, dispatch] = useContext<UserListContextResult>(UserListContext)
  const [isHover, useHover] = useState(true)
  const {currentUserID, orgID} = context

  const isCurrentUser = user.id === currentUserID

  const handleRemove = async () => {
    removeUser(dispatch, user, orgID)
  }

  const componentStatus =
    user.status === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  if (componentStatus === ComponentStatus.Loading) {
    return (
      <IndexList.Cell alignment={Alignment.Right}>
        <TechnoSpinner diameterPixels={30} strokeWidth={ComponentSize.Small} />
      </IndexList.Cell>
    )
  }

  return (
    <IndexList.Cell revealOnHover={isHover} alignment={Alignment.Right}>
      {!isCurrentUser && (
        <ConfirmationButton
          icon={IconFont.Trash}
          onShow={() => useHover(false)}
          onHide={() => useHover(true)}
          status={componentStatus}
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
  )
}

export default UserListContextMenu
