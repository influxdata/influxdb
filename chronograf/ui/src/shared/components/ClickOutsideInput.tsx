import React, {Component, MouseEvent, ChangeEvent} from 'react'

import onClickOutside from 'src/shared/components/OnClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  min?: string
  max?: string
  id: string
  type: string
  customPlaceholder: string
  customValue: string
  onGetRef: (el: HTMLInputElement) => void
  onFocus: () => void
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
  onKeyDown: () => void
  handleClickOutsideInput: (e: MouseEvent<HTMLElement>) => void
}

@ErrorHandling
class ClickOutsideInput extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {
      id,
      min = '',
      type,
      onFocus,
      onChange,
      onGetRef,
      onKeyDown,
      customValue,
      customPlaceholder,
      max,
    } = this.props

    return (
      <input
        className="form-control input-sm"
        id={id}
        min={min}
        max={max}
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

  public handleClickOutside = (e): void => {
    this.props.handleClickOutsideInput(e)
  }
}

export default onClickOutside(ClickOutsideInput)
