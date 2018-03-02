import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import Notification from 'src/shared/components/Notification'

const Notifications = ({notifications}) =>
  <div className="notification-center">
    {notifications.map(n => <Notification key={n.id} notification={n} />)}
  </div>

const {arrayOf, number, shape, string} = PropTypes

Notifications.propTypes = {
  notifications: arrayOf(
    shape({
      id: string.isRequired,
      type: string.isRequired,
      message: string.isRequired,
      created: number.isRequired,
      duration: number.isRequired,
      icon: string,
    })
  ),
}

const mapStateToProps = ({notifications}) => ({
  notifications,
})

export default connect(mapStateToProps, null)(Notifications)
