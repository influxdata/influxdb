import React, {SFC} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import uuid from 'uuid'

import ReactTooltip from 'react-tooltip'

import {Source} from 'src/types'

interface Props {
  sourceOverride?: Source
}

const SourceIndicator: SFC<Props> = (
  {sourceOverride},
  {source: {name, url}}
) => {
  const sourceName: string = _.get(sourceOverride, 'name', name)
  const sourceUrl: string = _.get(sourceOverride, 'url', url)
  const sourceNameTooltip: string = `<h1>Connected to Source:</h1><p><code>${sourceName} @ ${sourceUrl}</code></p>`
  const uuidTooltip: string = uuid.v4()

  return sourceName ? (
    <div
      className="source-indicator"
      data-for={uuidTooltip}
      data-tip={sourceNameTooltip}
    >
      <span className="icon disks" />
      <ReactTooltip
        id={uuidTooltip}
        effect="solid"
        html={true}
        place="left"
        class="influx-tooltip"
      />
    </div>
  ) : null
}

const {shape} = PropTypes

SourceIndicator.contextTypes = {
  source: shape({}),
}

export default SourceIndicator
