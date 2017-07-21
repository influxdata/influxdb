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
      <div className="graph-empty">
        <p>
          The current source does not have an associated Kapacitor instance
          <br />
          <br />
          <Link to={path} className="btn btn-sm btn-primary">
            Configure Kapacitor
          </Link>
        </p>
      </div>
    )
  },
})

export default NoKapacitorError
