import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import classnames from 'classnames'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

import {changeCellType} from 'src/dashboards/actions/cellEditorOverlay'

const GraphTypeSelector = ({type, handleChangeCellType}) => {
  const onChangeCellType = newType => () => {
    handleChangeCellType(newType)
  }

  return (
    <FancyScrollbar
      className="display-options--cell display-options--cellx2"
      autoHide={false}
    >
      <div className="display-options--cell-wrapper">
        <h5 className="display-options--header">Visualization Type</h5>
        <div className="viz-type-selector">
          {GRAPH_TYPES.map(graphType => (
            <div
              key={graphType.type}
              className={classnames('viz-type-selector--option', {
                active: graphType.type === type,
              })}
            >
              <div onClick={onChangeCellType(graphType.type)}>
                {graphType.graphic}
                <p>{graphType.menuOption}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </FancyScrollbar>
  )
}

const {func, string} = PropTypes

GraphTypeSelector.propTypes = {
  type: string.isRequired,
  handleChangeCellType: func.isRequired,
}

const mapStateToProps = ({
  cellEditorOverlay: {
    cell: {type},
  },
}) => ({
  type,
})

const mapDispatchToProps = dispatch => ({
  handleChangeCellType: bindActionCreators(changeCellType, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(GraphTypeSelector)
