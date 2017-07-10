import React, {PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import Ranger from 'src/dashboards/components/Ranger'

const style = {
  height: '100%',
  margin: '0 60px',
  display: 'flex',
  backgroundColor: '#202028',
  justifyContent: 'space-around',
}

const DisplayOptions = ({
  selectedGraphType,
  onSelectGraphType,
  onSetRange,
  yRange,
}) => (
  <div className="display-options" style={style}>
    <GraphTypeSelector
      selectedGraphType={selectedGraphType}
      onSelectGraphType={onSelectGraphType}
    />
    <Ranger onSetRange={onSetRange} yRange={yRange} />
  </div>
)

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
