import React, {PureComponent} from 'react'
import {Link} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'
import {get} from 'lodash'

//Actions
import {dismissNotification as dismissNotificationAction} from 'src/shared/actions/notifications'

import {Notification, ComponentSize, Gradients} from '@influxdata/clockface'

//Types
import {AppState, NotificationStyle} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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

const mstp = ({notifications}: AppState) => ({
  notifications,
})

const mdtp = {
  dismissNotification: dismissNotificationAction,
}

const connector = connect(mstp, mdtp)

export default connector(Notifications)
