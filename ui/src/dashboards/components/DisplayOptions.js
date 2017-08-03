import React, {PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import AxesOptions from 'src/dashboards/components/AxesOptions'

const DisplayOptions = ({
  selectedGraphType,
  onSelectGraphType,
  onSetLabel,
  onSetRange,
  axes,
}) =>
  <div className="display-options">
    <GraphTypeSelector
      selectedGraphType={selectedGraphType}
      onSelectGraphType={onSelectGraphType}
    />
    <AxesOptions onSetLabel={onSetLabel} onSetRange={onSetRange} axes={axes} />
  </div>

const {func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  onSetRange: func.isRequired,
  onSetLabel: func.isRequired,
  axes: shape({}).isRequired,
}

export default DisplayOptions
