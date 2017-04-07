import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import DataExplorer from './DataExplorer'

const App = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  render() {
    return (
      <div className="page">
        <DataExplorer source={this.props.source} />
      </div>
    )
  },
})

export default withRouter(App)
