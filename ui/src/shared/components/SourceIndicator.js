import React, {PropTypes} from 'react'

const SourceIndicator = React.createClass({
  propTypes: {
    sourceName: PropTypes.string,
  },

  render() {
    const {sourceName} = this.props
    if (!sourceName) {
      return null
    }
    return (
      <div className="source-indicator">
        <span className="icon server2"></span>
        {sourceName}
      </div>
    )
  },
})

export default SourceIndicator
