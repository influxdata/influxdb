// Libraries
import React, {FC} from 'react'

// Components
import {
  Button,
  IconFont,
  ButtonType,
  ComponentColor,
} from '@influxdata/clockface'

// Constants
import {roles} from 'src/users/constants'

// Types
import {DraftInvite} from 'src/types'

interface Props {
  draftInvite: DraftInvite
}

const UserInviteSubmit: FC<Props> = ({draftInvite}) => {
  const isRoleSelected = roles.includes(draftInvite.role)

  const getTitleText = () => {
    if (!draftInvite.email && !isRoleSelected) {
      return 'Please enter email and select a role'
    }

    if (!draftInvite.email) {
      return 'Please enter email'
    }

    if (!isRoleSelected) {
      return 'Please select a role'
    }

    return ''
  }

  return (
    <Button
      icon={IconFont.Plus}
      text="Add &amp; Invite"
      color={ComponentColor.Primary}
      type={ButtonType.Submit}
      titleText={getTitleText()}
      className="user-list-invite--button"
    />
  )
}

export default UserInviteSubmit
