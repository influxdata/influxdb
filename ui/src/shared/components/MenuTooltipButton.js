import React, {Component} from 'react'
import PropTypes from 'prop-types'
import OnClickOutside from 'react-onclickoutside'
import classnames from 'classnames'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class MenuTooltipButton extends Component {
  state = {
    expanded: false,
  }

  handleButtonClick = () => {
    const {informParent} = this.props

    this.setState({expanded: !this.state.expanded})
    informParent()
  }

  handleMenuItemClick = menuItemAction => () => {
    const {informParent} = this.props

    this.setState({expanded: false})
    menuItemAction()
    informParent()
  }

  handleClickOutside = () => {
    const {informParent} = this.props
    const {expanded} = this.state

    if (expanded === false) {
      return
    }

    this.setState({expanded: false})
    informParent()
  }

  renderMenuOptions = () => {
    const {menuOptions} = this.props
    const {expanded} = this.state

    if (expanded === false) {
      return null
    }

    return menuOptions.map((option, i) => (
      <div
        key={i}
        className={`dash-graph-context--menu-item${
          option.disabled ? ' disabled' : ''
        }`}
        onClick={
          option.disabled ? null : this.handleMenuItemClick(option.action)
        }
      >
        {option.text}
      </div>
    ))
  }

  render() {
    const {icon, theme} = this.props
    const {expanded} = this.state

    return (
      <div
        className={classnames('dash-graph-context--button', {active: expanded})}
        onClick={this.handleButtonClick}
      >
        <span className={`icon ${icon}`} />
        {expanded ? (
          <div className={`dash-graph-context--menu ${theme}`}>
            {this.renderMenuOptions()}
          </div>
        ) : null}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

MenuTooltipButton.defaultProps = {
  theme: 'default',
}

MenuTooltipButton.propTypes = {
  theme: string, // accepted values: default, primary, warning, success, danger
  icon: string.isRequired,
  menuOptions: arrayOf(
    shape({
      text: string.isRequired,
      action: func.isRequired,
      disabled: bool,
    })
  ).isRequired,
  informParent: func,
}

export default OnClickOutside(MenuTooltipButton)
