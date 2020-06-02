// Libraries
import React, {useContext, Dispatch} from 'react'

// Components
import {
  ComponentSize,
  Gradients,
  IconFont,
  Notification,
  RemoteDataState,
} from '@influxdata/clockface'
import {UserListContext} from './UserListContainer'

// Actions
import {
  UserListState,
  Action,
  removeUserStatus as removeUserStatusAction,
  removeInviteStatus as removeInviteStatusAction,
  resendInviteStatus as resendInviteStatusAction,
} from '../reducers'

export default function UserListNotifications() {
  const [
    {removeUserStatus, removeInviteStatus, resendInviteStatus},
    dispatch,
  ] = useContext<[UserListState, Dispatch<Action>]>(UserListContext)

  const hideRemoveUserNotify = () => {
    dispatch(removeUserStatusAction(RemoteDataState.NotStarted))
  }

  const hideWithdrawNotify = () => {
    dispatch(removeInviteStatusAction(RemoteDataState.NotStarted))
  }

  const hideResendInviteStatus = () => {
    dispatch(resendInviteStatusAction(RemoteDataState.NotStarted))
  }

  return (
    <>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.HotelBreakfast}
        icon={IconFont.UserRemove}
        onDismiss={hideWithdrawNotify}
        onTimeout={hideWithdrawNotify}
        visible={removeInviteStatus === RemoteDataState.Done}
        duration={5000}
      >
        Invitation Withdrawn
      </Notification>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.LASunset}
        icon={IconFont.Stop}
        onDismiss={hideWithdrawNotify}
        onTimeout={hideWithdrawNotify}
        visible={removeInviteStatus === RemoteDataState.Error}
        duration={2000}
      >
        Error withdrawing invite, try again
      </Notification>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.HotelBreakfast}
        icon={IconFont.UserAdd}
        onDismiss={hideResendInviteStatus}
        onTimeout={hideResendInviteStatus}
        visible={resendInviteStatus === RemoteDataState.Done}
        duration={5000}
      >
        Invitation Sent
      </Notification>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.LASunset}
        icon={IconFont.Stop}
        onDismiss={hideResendInviteStatus}
        onTimeout={hideResendInviteStatus}
        visible={resendInviteStatus === RemoteDataState.Error}
        duration={5000}
      >
        Error sending invitation
      </Notification>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.HotelBreakfast}
        icon={IconFont.UserRemove}
        onDismiss={hideRemoveUserNotify}
        onTimeout={hideRemoveUserNotify}
        visible={removeUserStatus === RemoteDataState.Done}
        duration={2000}
      >
        User Removed
      </Notification>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.LASunset}
        icon={IconFont.Stop}
        onDismiss={hideRemoveUserNotify}
        onTimeout={hideRemoveUserNotify}
        visible={removeUserStatus === RemoteDataState.Error}
        duration={2000}
      >
        Error removing user, try again
      </Notification>
    </>
  )
}
