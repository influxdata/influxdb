import React from 'react'

const {node} = React.PropTypes
const PanelHeading = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  render() {
    return (
      <div className="panel-heading text-center">
        <h2 className="deluxe">
          {this.props.children}
        </h2>
      </div>
    )
  },
})

export default PanelHeading
