import React, {Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import ColorScaleDropdown from 'src/shared/components/ColorScaleDropdown'

import {updateLineColors} from 'src/dashboards/actions/cellEditorOverlay'
import {ColorNumber} from 'src/types/colors'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  lineColors: ColorNumber[]
  handleUpdateLineColors: (colors: ColorNumber[]) => void
}

@ErrorHandling
class LineGraphColorSelector extends Component<Props> {
  public render() {
    const {lineColors} = this.props

    return (
      <div className="form-group col-xs-12">
        <label>Line Colors</label>
        <ColorScaleDropdown
          onChoose={this.handleSelectColors}
          stretchToFit={true}
          selected={lineColors}
        />
      </div>
    )
  }

  public handleSelectColors = (colorScale): void => {
    const {handleUpdateLineColors} = this.props
    const {colors} = colorScale

    handleUpdateLineColors(colors)
  }
}

const mapStateToProps = ({cellEditorOverlay: {lineColors}}) => ({
  lineColors,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateLineColors: bindActionCreators(updateLineColors, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  LineGraphColorSelector
)
