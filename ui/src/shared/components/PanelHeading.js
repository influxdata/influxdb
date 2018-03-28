import React from 'react'
import PropTypes from 'prop-types'

const {node} = PropTypes
const PanelHeading = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  render() {
    return (
      <div className="panel-heading text-center">
        <h2 className="deluxe">{this.props.children}</h2>
      </div>
    )
  },
})

export default PanelHeading
