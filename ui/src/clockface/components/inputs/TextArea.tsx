// Libraries
import React, {
  Component,
  CSSProperties,
  ChangeEvent,
  KeyboardEvent,
} from 'react'
import classnames from 'classnames'

// Types
import {ComponentStatus, ComponentSize} from 'src/clockface/types'

export enum AutoComplete {
  On = 'on',
  Off = 'off',
}

export enum AutoCapitalize {
  None = 'none',
  Sentences = 'sentences',
  Words = 'words',
  Characters = 'characters',
  On = 'on',
  Off = 'off',
}

export enum Wrap {
  Hard = 'hard',
  Soft = 'soft',
  Off = 'off',
}

interface Props {
  autocapitalize: AutoCapitalize
  autocomplete: AutoComplete
  autofocus: boolean
  cols: number
  disabled: boolean
  form: string
  maxlength: number
  minlength: number
  name: string
  placeholder: string
  readOnly: boolean
  required: boolean
  rows: number
  spellCheck: boolean
  wrap: Wrap.Off
  widthPixels?: number
  size?: ComponentSize
  status?: ComponentStatus
  value: string
  customClass?: string
  onChange?: (s: string) => void
  onBlur?: (e?: ChangeEvent<HTMLTextAreaElement>) => void
  onFocus?: (e?: ChangeEvent<HTMLTextAreaElement>) => void
  onKeyPress?: (e: KeyboardEvent<HTMLTextAreaElement>) => void
  onKeyUp?: (e: KeyboardEvent<HTMLTextAreaElement>) => void
  onKeyDown?: (e: KeyboardEvent<HTMLTextAreaElement>) => void
}

class TextArea extends Component<Props> {
  public static defaultProps = {
    autocapitalize: AutoCapitalize.Off,
    autocomplete: AutoComplete.Off,
    autofocus: false,
    cols: 20,
    disabled: false,
    form: '',
    maxlength: null,
    minlength: null,
    name: '',
    placeholder: '',
    readOnly: false,
    required: false,
    rows: 20,
    spellCheck: false,
    wrap: Wrap.Off,
    value: '',
  }

  public render() {
    const {
      autocomplete,
      autofocus,
      cols,
      status,
      form,
      maxlength,
      minlength,
      name,
      placeholder,
      readOnly,
      required,
      rows,
      spellCheck,
      wrap,
      value,
      onBlur,
      onFocus,
      onKeyPress,
      onKeyUp,
      onKeyDown,
    } = this.props

    return (
      <div className={this.className} style={this.containerStyle}>
        <textarea
          autoComplete={autocomplete}
          autoFocus={autofocus}
          cols={cols}
          disabled={status === ComponentStatus.Disabled}
          form={form}
          maxLength={maxlength}
          minLength={minlength}
          name={name}
          placeholder={placeholder}
          readOnly={readOnly}
          required={required}
          rows={rows}
          spellCheck={spellCheck}
          wrap={wrap}
          className="text-area"
          value={value}
          onBlur={onBlur}
          onFocus={onFocus}
          onKeyPress={onKeyPress}
          onKeyUp={onKeyUp}
          onKeyDown={onKeyDown}
          onChange={this.handleChange}
        />
      </div>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const {onChange} = this.props

    if (onChange) {
      onChange(e.target.value)
    }
  }

  private get className(): string {
    const {size, customClass} = this.props

    return classnames('text-area--container', {
      [`input-${size}`]: size,
      [`customClass`]: customClass,
    })
  }

  private get containerStyle(): CSSProperties {
    const {widthPixels} = this.props

    if (widthPixels) {
      return {width: `${widthPixels}px`}
    }

    return {width: '100%'}
  }
}

export default TextArea
