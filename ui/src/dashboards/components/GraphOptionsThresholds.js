import React, {PropTypes} from 'react'
import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {
  GAUGE_COLORS,
  SINGLE_STAT_BASE,
} from 'src/dashboards/constants/gaugeColors'

const GraphOptionsThresholds = ({
  handleAddThreshold,
  disableAddThreshold,
  sortedColors,
  formatColor,
  handleChooseColor,
  handleValidateColorValue,
  handleUpdateColorValue,
  handleDeleteThreshold,
}) => {
  return (
    <div>
      <label>Thresholds</label>
      <button
        className="btn btn-sm btn-primary gauge-controls--add-threshold"
        onClick={handleAddThreshold}
        disabled={disableAddThreshold}
      >
        <span className="icon plus" /> Add Threshold
      </button>
      {sortedColors.map(
        color =>
          color.id === SINGLE_STAT_BASE
            ? <div className="gauge-controls--section" key={color.id}>
                <div className="gauge-controls--label">Base Color</div>
                <ColorDropdown
                  colors={GAUGE_COLORS}
                  selected={formatColor(color)}
                  onChoose={handleChooseColor(color)}
                  stretchToFit={true}
                />
              </div>
            : <Threshold
                visualizationType="single-stat"
                threshold={color}
                key={color.id}
                onChooseColor={handleChooseColor}
                onValidateColorValue={handleValidateColorValue}
                onUpdateColorValue={handleUpdateColorValue}
                onDeleteThreshold={handleDeleteThreshold}
              />
      )}
    </div>
  )
}
const {arrayOf, bool, func, shape, string, number} = PropTypes

GraphOptionsThresholds.propTypes = {
  handleAddThreshold: func.isRequired,
  disableAddThreshold: bool,
  sortedColors: arrayOf(
    shape({
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      type: string.isRequired,
      value: number.isRequired,
    }).isRequired
  ).isRequired,
  formatColor: func.isRequired,
  handleChooseColor: func.isRequired,
  handleValidateColorValue: func.isRequired,
  handleUpdateColorValue: func.isRequired,
  handleDeleteThreshold: func.isRequired,
}

export default GraphOptionsThresholds
