import React, {Component, PropTypes} from 'react'

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
    const {colors, selected} = this.props

    const dropdownClassNames = visible
      ? 'color-dropdown open'
      : 'color-dropdown'
    const toggleClassNames = visible
      ? 'btn btn-sm btn-default color-dropdown--toggle active'
      : 'btn btn-sm btn-default color-dropdown--toggle'

    return (
      <div className={dropdownClassNames}>
        <div className={toggleClassNames} onClick={this.handleToggleMenu}>
          <div
            className="color-dropdown--swatch"
            style={{backgroundColor: selected.hex}}
          />
          <div className="color-dropdown--name">
            {selected.text}
          </div>
          <span className="caret" />
        </div>
        {visible
          ? <div className="color-dropdown--menu">
              <FancyScrollbar autoHide={false} autoHeight={true}>
                {colors.map((color, i) =>
                  <div
                    className={
                      color.text === selected.text
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
                      {color.text}
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

const {arrayOf, func, shape, string} = PropTypes

ColorDropdown.propTypes = {
  selected: shape({
    hex: string.isRequired,
    text: string.isRequired,
  }).isRequired,
  onChoose: func.isRequired,
  colors: arrayOf(
    shape({
      hex: string.isRequired,
      text: string.isRequired,
    })
  ).isRequired,
}

export default OnClickOutside(ColorDropdown)
