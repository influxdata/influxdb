import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
// import Notification from 'src/shared/components/notifications/Notification'
//Actions
import {dismissNotification as dismissNotificationAction} from 'src/shared/actions/notifications'

import {
  Notification,
  ComponentSize,
  Gradients,
  ComponentColor,
} from '@influxdata/clockface'

//Types
import {Notification as NotificationType} from 'src/types/notifications'

interface StateProps {
  notifications: NotificationType[]
}

interface DispatchProps {
  dismissNotification: typeof dismissNotificationAction
}

type Props = StateProps & DispatchProps

const matchGradientToColor = (color: ComponentColor): Gradients => {
  switch (color) {
    case ComponentColor.Primary:
      return Gradients.Primary
    case ComponentColor.Warning:
      return Gradients.WarningLight
    case ComponentColor.Success:
      return Gradients.HotelBreakfast
    case ComponentColor.Danger:
      return Gradients.DangerDark
    case ComponentColor.Default:
    default:
      return Gradients.DefaultLight
  }
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
