import React, {Component, PropTypes} from 'react'

import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbar from 'shared/components/FancyScrollbar'

class ColorDropdown extends Component {
  constructor(props) {
    super(props)

    this.state = {
      visible: false,
    }
  }

  handleToggleMenu = () => {
    const {disabled} = this.props

    if (disabled) {
      return
    }
    this.setState({visible: !this.state.visible})
  }

  handleClickOutside = () => {
    this.setState({visible: false})
  }

  handleColorClick = color => () => {
    this.props.onChoose(color)
    this.setState({visible: false})
  }

  render() {
    const {visible} = this.state
    const {colors, selected, disabled} = this.props

    const dropdownClassNames = visible
      ? 'color-dropdown open'
      : 'color-dropdown'
    const toggleClassNames = classnames(
      'btn btn-sm btn-default color-dropdown--toggle',
      {active: visible, 'color-dropdown__disabled': disabled}
    )

    return (
      <div className={dropdownClassNames}>
        <div
          className={toggleClassNames}
          onClick={this.handleToggleMenu}
          disabled={disabled}
        >
          <div
            className="color-dropdown--swatch"
            style={{backgroundColor: selected.hex}}
          />
          <div className="color-dropdown--name">
            {selected.name}
          </div>
          <span className="caret" />
        </div>
        {visible
          ? <div className="color-dropdown--menu">
              <FancyScrollbar autoHide={false} autoHeight={true}>
                {colors.map((color, i) =>
                  <div
                    className={
                      color.name === selected.name
                        ? 'color-dropdown--item active'
                        : 'color-dropdown--item'
                    }
                    key={i}
                    onClick={this.handleColorClick(color)}
                  >
                    <span
                      className="color-dropdown--swatch"
                      style={{backgroundColor: color.hex}}
                    />
                    <span className="color-dropdown--name">
                      {color.name}
                    </span>
                  </div>
                )}
              </FancyScrollbar>
            </div>
          : null}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

ColorDropdown.propTypes = {
  selected: shape({
    hex: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  onChoose: func.isRequired,
  colors: arrayOf(
    shape({
      hex: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
  disabled: bool,
}

export default OnClickOutside(ColorDropdown)
