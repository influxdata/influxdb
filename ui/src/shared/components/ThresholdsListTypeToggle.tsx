import React, {Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {updateThresholdsListType} from 'src/dashboards/actions/cellEditorOverlay'

import {
  THRESHOLD_TYPE_TEXT,
  THRESHOLD_TYPE_BG,
} from 'src/shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PropsFromRedux {
  thresholdsListType: string
}
interface PropsFromParent {
  containerClass: string
  handleUpdateThresholdsListType: (newType: string) => void
}

type Props = PropsFromRedux & PropsFromParent

@ErrorHandling
class ThresholdsListTypeToggle extends Component<Props> {
  public render() {
    const {containerClass} = this.props

    return (
      <div className={containerClass}>
        <label>Threshold Coloring</label>
        <ul className="nav nav-tablist nav-tablist-sm">
          <li
            className={this.bgTabClassName}
            onClick={this.handleToggleThresholdsListType(THRESHOLD_TYPE_BG)}
          >
            Background
          </li>
          <li
            className={this.textTabClassName}
            onClick={this.handleToggleThresholdsListType(THRESHOLD_TYPE_TEXT)}
          >
            Text
          </li>
        </ul>
      </div>
    )
  }

  private get bgTabClassName(): string {
    const {thresholdsListType} = this.props

    if (thresholdsListType === THRESHOLD_TYPE_BG) {
      return 'active'
    }

    return ''
  }

  private get textTabClassName(): string {
    const {thresholdsListType} = this.props

    if (thresholdsListType === THRESHOLD_TYPE_TEXT) {
      return 'active'
    }

    return ''
  }

  private handleToggleThresholdsListType = (newType: string) => (): void => {
    const {handleUpdateThresholdsListType} = this.props

    handleUpdateThresholdsListType(newType)
  }
}

const mapStateToProps = ({
  cellEditorOverlay: {thresholdsListType},
}): PropsFromRedux => ({
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
