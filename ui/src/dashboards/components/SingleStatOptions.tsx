import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {updateAxes} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Axes} from 'src/types'

interface Props {
  handleUpdateAxes: (axes: Axes) => void
  axes: Axes
  onResetFocus: () => void
}

@ErrorHandling
class SingleStatOptions extends PureComponent<Props> {
  public render() {
    const {
      axes: {
        y: {prefix, suffix},
      },
      onResetFocus,
    } = this.props

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
                maxLength={5}
              />
            </div>
            <div className="form-group col-xs-6">
              <label>Suffix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={suffix}
                onChange={this.handleUpdateSuffix}
                maxLength={5}
              />
            </div>
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }

  private handleUpdatePrefix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, prefix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  private handleUpdateSuffix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, suffix: e.target.value}}

    handleUpdateAxes(newAxes)
  }
}

const mstp = ({cellEditorOverlay}) => ({
  axes: cellEditorOverlay.cell.axes,
})

const mdtp = {
  handleUpdateAxes: updateAxes,
}

export default connect(mstp, mdtp)(SingleStatOptions)
