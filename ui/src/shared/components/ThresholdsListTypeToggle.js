import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {updateThresholdsListType} from 'src/dashboards/actions/cellEditorOverlay'

import {
  THRESHOLD_TYPE_TEXT,
  THRESHOLD_TYPE_BG,
} from 'shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class ThresholdsListTypeToggle extends Component {
  handleToggleThresholdsListType = newType => () => {
    const {handleUpdateThresholdsListType} = this.props

    handleUpdateThresholdsListType(newType)
  }

  render() {
    const {thresholdsListType, containerClass} = this.props

    return (
      <div className={containerClass}>
        <label>Threshold Coloring</label>
        <ul className="nav nav-tablist nav-tablist-sm">
          <li
            className={`${
              thresholdsListType === THRESHOLD_TYPE_BG ? 'active' : ''
            }`}
            onClick={this.handleToggleThresholdsListType(THRESHOLD_TYPE_BG)}
          >
            Background
          </li>
          <li
            className={`${
              thresholdsListType === THRESHOLD_TYPE_TEXT ? 'active' : ''
            }`}
            onClick={this.handleToggleThresholdsListType(THRESHOLD_TYPE_TEXT)}
          >
            Text
          </li>
        </ul>
      </div>
    )
  }
}
const {func, string} = PropTypes

ThresholdsListTypeToggle.propTypes = {
  thresholdsListType: string.isRequired,
  handleUpdateThresholdsListType: func.isRequired,
  containerClass: string.isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {thresholdsListType}}) => ({
  thresholdsListType,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateThresholdsListType: bindActionCreators(
    updateThresholdsListType,
    dispatch
  ),
})
export default connect(mapStateToProps, mapDispatchToProps)(
  ThresholdsListTypeToggle
)
