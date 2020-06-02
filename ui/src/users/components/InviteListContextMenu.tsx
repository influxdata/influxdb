import React, {FC, useState, useContext} from 'react'

import {
  RemoteDataState,
  IndexList,
  TechnoSpinner,
  Alignment,
  ComponentSize,
  FlexBox,
  IconFont,
  ComponentColor,
  ButtonShape,
  SquareButton,
  ConfirmationButton,
  ComponentStatus,
} from '@influxdata/clockface'
import {UserListContext, UserListContextResult} from './UserListContainer'

// Thunks
import {withdrawInvite, resendInvite} from '../thunks'

// Types
import {Invite} from 'src/types'

interface Props {
  invite: Invite
}

const InviteListContextMenu: FC<Props> = ({invite}) => {
  const [isHover, useHover] = useState(true)
  const [{orgID}, dispatch] = useContext<UserListContextResult>(UserListContext)

  const handleRemove = () => {
    withdrawInvite(dispatch, orgID, invite)
  }

  const handleResend = () => {
    resendInvite(dispatch, orgID, invite.id)
  }

  const handleShow = () => {
    useHover(false)
  }

  const handleHide = () => {
    useHover(true)
  }

  const componentStatus =
    invite.status === RemoteDataState.Loading
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
      <FlexBox margin={ComponentSize.Small}>
        <SquareButton
          titleText="Resend Invitation"
          icon={IconFont.Refresh}
          color={ComponentColor.Secondary}
          onClick={handleResend}
        />
        <ConfirmationButton
          icon={IconFont.Trash}
          onShow={handleShow}
          onHide={handleHide}
          confirmationLabel="This action will invalidate the invitation link sent to this user"
          confirmationButtonText="Withdraw Invitation"
          titleText="Withdraw Invitation"
          confirmationButtonColor={ComponentColor.Danger}
          color={ComponentColor.Danger}
          shape={ButtonShape.Square}
          onConfirm={handleRemove}
          status={componentStatus}
        />
      </FlexBox>
    </IndexList.Cell>
  )

  return
}

export default InviteListContextMenu
