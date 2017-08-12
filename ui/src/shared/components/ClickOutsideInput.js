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
      id,
      type,
      customPlaceholder,
      onGetRef,
      customValue,
      onFocus,
      onChange,
      onKeyDown,
    } = this.props

    return (
      <input
        className="form-control input-sm"
        id={id}
        type={type}
        name={customPlaceholder}
        ref={onGetRef}
        value={customValue}
        onFocus={onFocus}
        onChange={onChange}
        onKeyDown={onKeyDown}
        placeholder={customPlaceholder}
      />
    )
  }
}

const {func, string} = PropTypes

ClickOutsideInput.propTypes = {
  id: string.isRequired,
  type: string.isRequired,
  customPlaceholder: string.isRequired,
  customValue: string.isRequired,
  onGetRef: func.isRequired,
  onFocus: func.isRequired,
  onChange: func.isRequired,
  onKeyDown: func.isRequired,
  handleClickOutsideCustomValueInput: func.isRequired,
}

export default onClickOutside(ClickOutsideInput)
