import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

const SourceIndicator = React.createClass({
  propTypes: {
    sourceName: PropTypes.string,
  },

  render() {
    const {sourceName} = this.props
    if (!sourceName) {
      return null
    }
    const sourceNameTooltip = `Connected to <code>${sourceName}</code>`
    return (
      <div
        className="source-indicator"
        data-for="source-indicator-tooltip"
        data-tip={sourceNameTooltip}
      >
        <span className="icon server2" />
        <ReactTooltip
          id="source-indicator-tooltip"
          effect="solid"
          html={true}
          offset={{top: 2}}
          place="bottom"
          class="influx-tooltip place-bottom"
        />
      </div>
    )
  },
})

export default SourceIndicator
