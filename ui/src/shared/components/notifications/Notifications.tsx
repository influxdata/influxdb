import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

//Actions
import {dismissNotification as dismissNotificationAction} from 'src/shared/actions/notifications'

import {Notification, ComponentSize, Gradients} from '@influxdata/clockface'

//Types
import {
  Notification as NotificationType,
  NotificationStyle,
} from 'src/types/notifications'

interface StateProps {
  notifications: NotificationType[]
}

interface DispatchProps {
  dismissNotification: typeof dismissNotificationAction
}

type Props = StateProps & DispatchProps

const matchGradientToColor = (style: NotificationStyle): Gradients => {
  const converter = {
    [NotificationStyle.Primary]: Gradients.Primary,
    [NotificationStyle.Warning]: Gradients.WarningLight,
    [NotificationStyle.Success]: Gradients.HotelBreakfast,
    [NotificationStyle.Error]: Gradients.DangerDark,
    [NotificationStyle.Info]: Gradients.DefaultLight,
  }
  return get(converter, style, Gradients.DefaultLight)
}

class Notifications extends PureComponent<Props> {
  public static defaultProps = {
    notifications: [],
  }

  public render() {
    const {notifications} = this.props

    return (
      <>
        {notifications.map(({id, style, icon, duration, message}) => {
          const gradient = matchGradientToColor(style)

          return (
            <Notification
              key={id}
              id={id}
              icon={icon}
              duration={duration}
              size={ComponentSize.ExtraSmall}
              gradient={gradient}
              onTimeout={this.props.dismissNotification}
              onDismiss={this.props.dismissNotification}
              testID={`notification-${style}`}
            >
              {message}
            </Notification>
          )
        })}
      </>
    )
  }
}

const mapStateToProps = ({notifications}): StateProps => ({
  notifications,
})

const mdtp: DispatchProps = {
  dismissNotification: dismissNotificationAction,
}

export default connect(
  mapStateToProps,
  mdtp
)(Notifications)
