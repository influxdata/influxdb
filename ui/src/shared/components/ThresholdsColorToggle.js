import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {updateSingleStatType} from 'src/dashboards/actions/cellEditorOverlay'

import {SINGLE_STAT_TEXT, SINGLE_STAT_BG} from 'shared/constants/thresholds'

class ThresholdsColorToggle extends Component {
  handleToggleSingleStatType = newType => () => {
    const {handleUpdateSingleStatType} = this.props

    handleUpdateSingleStatType(newType)
  }

  render() {
    const {singleStatType, containerClass} = this.props

    return (
      <div className={containerClass}>
        <label>Coloring</label>
        <ul className="nav nav-tablist nav-tablist-sm">
          <li
            className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
            onClick={this.handleToggleSingleStatType(SINGLE_STAT_BG)}
          >
            Background
          </li>
          <li
            className={`${singleStatType === SINGLE_STAT_TEXT ? 'active' : ''}`}
            onClick={this.handleToggleSingleStatType(SINGLE_STAT_TEXT)}
          >
            Text
          </li>
        </ul>
      </div>
    )
  }
}
const {func, string} = PropTypes

ThresholdsColorToggle.propTypes = {
  singleStatType: string.isRequired,
  handleUpdateSingleStatType: func.isRequired,
  containerClass: string.isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {singleStatType}}) => ({
  singleStatType,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateSingleStatType: bindActionCreators(
    updateSingleStatType,
    dispatch
  ),
})
export default connect(mapStateToProps, mapDispatchToProps)(
  ThresholdsColorToggle
)
