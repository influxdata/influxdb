import React from 'react'
import PropTypes from 'prop-types'

export const KapacitorTasksPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string.isRequired, // 'influx-enterprise'
      username: PropTypes.string.isRequired,
      links: PropTypes.shape({
        kapacitors: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {}
  },

  render() {
    return <div className="kapacitorTasks">tasks</div>
  },
})

export default KapacitorTasksPage
