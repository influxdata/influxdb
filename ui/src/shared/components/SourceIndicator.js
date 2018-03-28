import React from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import uuid from 'uuid'

import ReactTooltip from 'react-tooltip'

const SourceIndicator = ({sourceOverride}, {source: {name, url}}) => {
  const sourceName = _.get(sourceOverride, 'name', null)
    ? sourceOverride.name
    : name
  const sourceUrl = _.get(sourceOverride, 'url', null)
    ? sourceOverride.url
    : url

  if (!sourceName) {
    return null
  }
  const sourceNameTooltip = `<h1>Connected to Source:</h1><p><code>${sourceName} @ ${sourceUrl}</code></p>`
  const uuidTooltip = uuid.v4()

  return (
    <div
      className="source-indicator"
      data-for={uuidTooltip}
      data-tip={sourceNameTooltip}
    >
      <span className="icon server2" />
      <ReactTooltip
        id={uuidTooltip}
        effect="solid"
        html={true}
        place="left"
        class="influx-tooltip"
      />
    </div>
  )
}

const {shape, string} = PropTypes

SourceIndicator.propTypes = {
  sourceOverride: shape({
    name: string,
    url: string,
  }),
}

SourceIndicator.contextTypes = {
  source: shape({
    name: string,
    url: string,
  }),
}

export default SourceIndicator
