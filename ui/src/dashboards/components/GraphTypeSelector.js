import React, {PropTypes} from 'react'
import classnames from 'classnames'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

const GraphTypeSelector = ({selectedGraphType, onSelectGraphType}) =>
  <FancyScrollbar
    className="display-options--cell display-options--cellx2"
    autoHide={false}
  >
    <div className="display-options--cell-wrapper">
      <h5 className="display-options--header">Visualization Type</h5>
      <div className="viz-type-selector">
        {GRAPH_TYPES.map(graphType =>
          <div
            key={graphType.type}
            className={classnames('viz-type-selector--option', {
              active: graphType.type === selectedGraphType,
            })}
          >
            <div onClick={onSelectGraphType(graphType.type)}>
              {graphType.graphic}
              <p>
                {graphType.menuOption}
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  </FancyScrollbar>

const {func, string} = PropTypes

GraphTypeSelector.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
}

export default GraphTypeSelector
