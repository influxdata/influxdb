import React, {Component} from 'react'
import PropTypes from 'prop-types'
import uuid from 'uuid'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {LINE_COLOR_SCALES} from 'src/shared/constants/graphColorPalettes'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class ColorScaleDropdown extends Component {
  constructor(props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  handleToggleMenu = () => {
    const {disabled} = this.props

    if (disabled) {
      return
    }
    this.setState({expanded: !this.state.expanded})
  }

  handleClickOutside = () => {
    this.setState({expanded: false})
  }

  handleDropdownClick = colorScale => () => {
    this.props.onChoose(colorScale)
    this.setState({expanded: false})
  }

  generateGradientStyle = colors => ({
    background: `linear-gradient(to right, ${colors[0].hex} 0%,${
      colors[1].hex
    } 50%,${colors[2].hex} 100%)`,
  })

  render() {
    const {expanded} = this.state
    const {selected, disabled, stretchToFit} = this.props

    const dropdownClassNames = classnames('color-dropdown', {
      open: expanded,
      'color-dropdown--stretch': stretchToFit,
    })
    const toggleClassNames = classnames(
      'btn btn-sm btn-default color-dropdown--toggle',
      {active: expanded, 'color-dropdown__disabled': disabled}
    )

    return (
      <div className={dropdownClassNames}>
        <div
          className={toggleClassNames}
          onClick={this.handleToggleMenu}
          disabled={disabled}
        >
          <div
            className="color-dropdown--swatches"
            style={this.generateGradientStyle(selected)}
          />
          <div className="color-dropdown--name">{selected[0].name}</div>
          <span className="caret" />
        </div>
        {expanded ? (
          <div className="color-dropdown--menu">
            <FancyScrollbar autoHide={false} autoHeight={true}>
              {LINE_COLOR_SCALES.map(colorScale => (
                <div
                  className={
                    colorScale.name === selected[0].name
                      ? 'color-dropdown--item active'
                      : 'color-dropdown--item'
                  }
                  key={uuid.v4()}
                  onClick={this.handleDropdownClick(colorScale)}
                >
                  <div
                    className="color-dropdown--swatches"
                    style={this.generateGradientStyle(colorScale.colors)}
                  />
                  <span className="color-dropdown--name">
                    {colorScale.name}
                  </span>
                </div>
              ))}
            </FancyScrollbar>
          </div>
        ) : null}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

ColorScaleDropdown.propTypes = {
  selected: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
    }).isRequired
  ).isRequired,
  onChoose: func.isRequired,
  stretchToFit: bool,
  disabled: bool,
}

export default OnClickOutside(ColorScaleDropdown)
