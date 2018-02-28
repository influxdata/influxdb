import React, {PropTypes} from 'react'
import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {
  GAUGE_COLORS,
  SINGLE_STAT_BASE,
} from 'src/dashboards/constants/gaugeColors'

const GraphOptionsThresholds = ({
  onAddThreshold,
  disableAddThreshold,
  sortedColors,
  formatColor,
  onChooseColor,
  onValidateColorValue,
  onUpdateColorValue,
  onDeleteThreshold,
}) => {
  return (
    <div>
      <label>Thresholds</label>
      <button
        className="btn btn-sm btn-primary gauge-controls--add-threshold"
        onClick={onAddThreshold}
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
                  onChoose={onChooseColor(color)}
                  stretchToFit={true}
                />
              </div>
            : <Threshold
                visualizationType="single-stat"
                threshold={color}
                key={color.id}
                onChooseColor={onChooseColor}
                onValidateColorValue={onValidateColorValue}
                onUpdateColorValue={onUpdateColorValue}
                onDeleteThreshold={onDeleteThreshold}
              />
      )}
    </div>
  )
}
const {arrayOf, bool, func, shape, string, number} = PropTypes

GraphOptionsThresholds.propTypes = {
  onAddThreshold: func,
  disableAddThreshold: bool,
  sortedColors: arrayOf(
    shape({
      hex: string,
      id: string,
      name: string,
      type: string,
      value: number,
    })
  ),
  formatColor: func,
  onChooseColor: func,
  onValidateColorValue: func,
  onUpdateColorValue: func,
  onDeleteThreshold: func,
}

export default GraphOptionsThresholds
