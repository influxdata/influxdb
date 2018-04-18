import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import ThresholdsList from 'shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'shared/components/ThresholdsListTypeToggle'

import {updateAxes} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class SingleStatOptions extends Component {
  handleUpdatePrefix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, prefix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  handleUpdateSuffix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, suffix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  render() {
    const {axes: {y: {prefix, suffix}}, onResetFocus} = this.props

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Single Stat Controls</h5>
          <ThresholdsList onResetFocus={onResetFocus} />
          <div className="graph-options-group form-group-wrapper">
            <div className="form-group col-xs-6">
              <label>Prefix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={prefix}
                onChange={this.handleUpdatePrefix}
                maxLength="5"
              />
            </div>
            <div className="form-group col-xs-6">
              <label>Suffix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={suffix}
                onChange={this.handleUpdateSuffix}
                maxLength="5"
              />
            </div>
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {func, shape} = PropTypes

SingleStatOptions.propTypes = {
  handleUpdateAxes: func.isRequired,
  axes: shape({}).isRequired,
  onResetFocus: func.isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {cell: {axes}}}) => ({
  axes,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateAxes: bindActionCreators(updateAxes, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(SingleStatOptions)
