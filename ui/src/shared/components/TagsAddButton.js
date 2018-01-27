import React, {Component, PropTypes} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'

class TagsAddButton extends Component {
  constructor(props) {
    super(props)

    this.state = {open: false}
  }

  handleButtonClick = () => {
    this.setState({open: !this.state.open})
  }

  handleMenuClick = item => () => {
    this.setState({open: false})
    this.props.onChoose(item)
  }

  handleClickOutside = () => {
    this.setState({open: false})
  }

  render() {
    const {open} = this.state
    const {items} = this.props

    const classname = `tags-add${open ? ' open' : ''}`
    return (
      <div className={classname} onClick={this.handleButtonClick}>
        <span className="icon plus" />
        <div className="tags-add--menu">
          {items.map((item, i) =>
            <div
              key={i}
              className="tags-add--menu-item"
              onClick={this.handleMenuClick(item)}
            >
              {item.text}
            </div>
          )}
        </div>
      </div>
    )
  }
}

const {array, func} = PropTypes

TagsAddButton.propTypes = {
  items: array.isRequired,
  onChoose: func.isRequired,
}

export default OnClickOutside(TagsAddButton)
