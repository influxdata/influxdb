import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'
import GraphOptionsDecimalPlaces from 'src/dashboards/components/GraphOptionsDecimalPlaces'

import {
  updateAxes,
  changeDecimalPlaces,
} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Axes} from 'src/types'
import {DecimalPlaces} from 'src/types/dashboards'

interface Props {
  axes: Axes
  decimalPlaces: DecimalPlaces
  onResetFocus: () => void
  onUpdateAxes: (axes: Axes) => void
  onUpdateDecimalPlaces: (decimalPlaces: DecimalPlaces) => void
}

@ErrorHandling
class SingleStatOptions extends PureComponent<Props> {
  public render() {
    const {
      axes: {
        y: {prefix, suffix},
      },
      onResetFocus,
      decimalPlaces,
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
            <GraphOptionsDecimalPlaces
              digits={decimalPlaces.digits}
              isEnforced={decimalPlaces.isEnforced}
              onDecimalPlacesChange={this.handleDecimalPlacesChange}
            />
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }

  private handleDecimalPlacesChange = (decimalPlaces: DecimalPlaces) => {
    const {onUpdateDecimalPlaces} = this.props
    onUpdateDecimalPlaces(decimalPlaces)
  }

  private handleUpdatePrefix = e => {
    const {onUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, prefix: e.target.value}}

    onUpdateAxes(newAxes)
  }

  private handleUpdateSuffix = e => {
    const {onUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, suffix: e.target.value}}

    onUpdateAxes(newAxes)
  }
}

const mstp = ({cellEditorOverlay}) => ({
  axes: cellEditorOverlay.cell.axes,
  decimalPlaces: cellEditorOverlay.cell.decimalPlaces,
})

const mdtp = {
  onUpdateAxes: updateAxes,
  onUpdateDecimalPlaces: changeDecimalPlaces,
}

export default connect(mstp, mdtp)(SingleStatOptions)
