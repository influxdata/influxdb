import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {updateThresholdsListType} from 'src/dashboards/actions/cellEditorOverlay'

import {SINGLE_STAT_TEXT, SINGLE_STAT_BG} from 'shared/constants/thresholds'

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
            className={`${thresholdsListType === SINGLE_STAT_BG
              ? 'active'
              : ''}`}
            onClick={this.handleToggleThresholdsListType(SINGLE_STAT_BG)}
          >
            Background
          </li>
          <li
            className={`${thresholdsListType === SINGLE_STAT_TEXT
              ? 'active'
              : ''}`}
            onClick={this.handleToggleThresholdsListType(SINGLE_STAT_TEXT)}
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
