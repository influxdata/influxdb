import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

const SourceIndicator = (_, {source: {name: sourceName, url}}) => {
  if (!sourceName) {
    return null
  }
  const sourceNameTooltip = `Connected to <code>${sourceName} @ ${url}</code>`
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
}

const {shape, string} = PropTypes

SourceIndicator.contextTypes = {
  source: shape({
    name: string,
    url: string,
  }),
}

export default SourceIndicator
