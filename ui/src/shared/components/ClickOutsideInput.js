import React, {Component, PropTypes} from 'react'

import onClickOutside from 'shared/components/OnClickOutside'

class ClickOutsideInput extends Component {
  constructor(props) {
    super(props)

    this.handleClickOutside = ::this.handleClickOutside
  }

  handleClickOutside(e) {
    this.props.handleClickOutsideCustomValueInput(e)
  }

  render() {
    const {
      type,
      customPlaceholder,
      onGetRef,
      customValue,
      onChange,
      onKeyPress,
    } = this.props

    return (
      <input
        className="form-control input-sm"
        type={type}
        name={customPlaceholder}
        ref={onGetRef}
        value={customValue}
        onChange={onChange}
        onKeyPress={onKeyPress}
        placeholder={customPlaceholder}
      />
    )
  }
}

const {func, string} = PropTypes

ClickOutsideInput.propTypes = {
  type: string.isRequired,
  customPlaceholder: string.isRequired,
  onGetRef: func.isRequired,
  customValue: string.isRequired,
  onChange: func.isRequired,
  onKeyPress: func.isRequired,
  handleClickOutsideCustomValueInput: func.isRequired,
}

export default onClickOutside(ClickOutsideInput)
