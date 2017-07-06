import React, {PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import Ranger from 'src/dashboards/components/Ranger'

const DisplayOptions = ({
  selectedGraphType,
  onSelectGraphType,
  onSetRange,
  yRange,
}) =>
  <div className="display-options">
    <GraphTypeSelector
      selectedGraphType={selectedGraphType}
      onSelectGraphType={onSelectGraphType}
    />
    <Ranger onSetRange={onSetRange} yRange={yRange} />
  </div>

const {func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  onSetRange: func.isRequired,
  yRange: shape({
    min: string,
    max: string,
  }).isRequired,
}

export default DisplayOptions
