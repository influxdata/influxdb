import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {Notification as NotificationType} from 'src/types/notifications'
import Notification from 'src/shared/components/notifications/Notification'

interface Props {
  inPresentationMode?: boolean
  notifications: NotificationType[]
}

class Notifications extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    inPresentationMode: false,
  }

  public render() {
    const {notifications} = this.props

    return (
      <div className={this.className}>
        {notifications.map(n => <Notification key={n.id} notification={n} />)}
      </div>
    )
  }

  private get className(): string {
    const {inPresentationMode} = this.props

    if (inPresentationMode) {
      return 'notification-center__presentation-mode'
    }

    return 'notification-center'
  }
}

const mapStateToProps = ({
  notifications,
  app: {
    ephemeral: {inPresentationMode},
  },
}): Props => ({
  notifications,
  inPresentationMode,
})

export default connect(mapStateToProps, null)(Notifications)
