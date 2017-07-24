import React, {PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import Ranger from 'src/dashboards/components/Ranger'

const DisplayOptions = ({
  selectedGraphType,
  onSelectGraphType,
  onSetRange,
  axes,
}) =>
  <div className="display-options">
    <GraphTypeSelector
      selectedGraphType={selectedGraphType}
      onSelectGraphType={onSelectGraphType}
    />
    <Ranger onSetRange={onSetRange} axes={axes} />
  </div>

const {func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  onSetRange: func.isRequired,
  axes: shape({}).isRequired,
}

export default DisplayOptions
