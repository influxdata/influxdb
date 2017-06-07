import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const NoKapacitorError = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
  },

  render() {
    const path = `/sources/${this.props.source.id}/kapacitors/new`
    return (
      <div>
        <p>
          The current source does not have an associated Kapacitor instance,
          please configure one.
        </p>
        <Link to={path}>Add Kapacitor</Link>
      </div>
    )
  },
})

export default NoKapacitorError
