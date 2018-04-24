import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import Notification from 'shared/components/Notification'

const Notifications = ({notifications, inPresentationMode}) => (
  <div
    className={`${
      inPresentationMode
        ? 'notification-center__presentation-mode'
        : 'notification-center'
    }`}
  >
    {notifications.map(n => <Notification key={n.id} notification={n} />)}
  </div>
)

const {arrayOf, bool, number, shape, string} = PropTypes

Notifications.propTypes = {
  notifications: arrayOf(
    shape({
      id: string.isRequired,
      type: string.isRequired,
      message: string.isRequired,
      duration: number.isRequired,
      icon: string,
    })
  ),
  inPresentationMode: bool,
}

const mapStateToProps = ({
  notifications,
  app: {
    ephemeral: {inPresentationMode},
  },
}) => ({
  notifications,
  inPresentationMode,
})

export default connect(mapStateToProps, null)(Notifications)
