import React, {Component, MouseEvent, ChangeEvent, KeyboardEvent} from 'react'

import {ClickOutside} from 'src/shared/components/ClickOutside'
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
  onKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void
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
      <ClickOutside onClickOutside={this.handleClickOutside}>
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
      </ClickOutside>
    )
  }

  public handleClickOutside = (e): void => {
    this.props.handleClickOutsideInput(e)
  }
}

export default ClickOutsideInput
