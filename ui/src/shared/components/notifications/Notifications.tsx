import React, {PureComponent} from 'react'
import {Link} from 'react-router-dom'
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
        {notifications.map(
          ({id, style, icon, duration, message, link, linkText}) => {
            const gradient = matchGradientToColor(style)

            let button

            if (link && linkText) {
              button = (
                <Link
                  to={link}
                  className="notification--button cf-button cf-button-xs cf-button-default"
                >
                  {linkText}
                </Link>
              )
            }

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
                <span className="notification--message">{message}</span>
                {button}
              </Notification>
            )
          }
        )}
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

export default connect(mapStateToProps, mdtp)(Notifications)
