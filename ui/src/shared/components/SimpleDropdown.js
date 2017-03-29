import React, {PropTypes} from 'react'
import classNames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'

const Dropdown = React.createClass({
  propTypes: {
    children: PropTypes.node.isRequired,
    items: PropTypes.arrayOf(PropTypes.shape({
      text: PropTypes.string.isRequired,
    })).isRequired,
    onChoose: PropTypes.func.isRequired,
    className: PropTypes.string,
  },
  getInitialState() {
    return {
      isOpen: false,
    }
  },
  handleClickOutside() {
    this.setState({isOpen: false})
  },
  handleSelection(item) {
    this.toggleMenu()
    this.props.onChoose(item)
  },
  toggleMenu(e) {
    if (e) {
      e.stopPropagation()
    }
    this.setState({isOpen: !this.state.isOpen})
  },
  render() {
    const self = this
    const {items, className} = self.props

    return (
      <div onClick={this.toggleMenu} className={classNames(`dropdown ${className}`, {open: self.state.isOpen})}>
        <div className="btn btn-sm btn-info dropdown-toggle">
          {this.props.children}
        </div>
        {self.state.isOpen ?
          <ul className="dropdown-menu show">
            {items.map((item, i) => {
              return (
                <li className="dropdown-item" key={i} onClick={() => self.handleSelection(item)}>
                  <a href="#">
                    {item.text}
                  </a>
                </li>
              )
            })}
          </ul>
          : null}
      </div>
    )
  },
})

export default OnClickOutside(Dropdown)
