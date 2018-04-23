import React, {Component} from 'react'
import PropTypes from 'prop-types'

import onClickOutside from 'shared/components/OnClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class ClickOutsideInput extends Component {
  constructor(props) {
    super(props)
  }

  handleClickOutside = e => {
    this.props.handleClickOutsideInput(e)
  }

  render() {
    const {
      id,
      min,
      type,
      onFocus,
      onChange,
      onGetRef,
      onKeyDown,
      customValue,
      customPlaceholder,
    } = this.props

    return (
      <input
        className="form-control input-sm"
        id={id}
        min={min}
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
  min: string,
  id: string.isRequired,
  type: string.isRequired,
  customPlaceholder: string.isRequired,
  customValue: string.isRequired,
  onGetRef: func.isRequired,
  onFocus: func.isRequired,
  onChange: func.isRequired,
  onKeyDown: func.isRequired,
  handleClickOutsideInput: func.isRequired,
}

export default onClickOutside(ClickOutsideInput)
