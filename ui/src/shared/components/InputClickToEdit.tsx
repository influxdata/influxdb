import React, {ChangeEvent, KeyboardEvent, PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  wrapperClass: string
  value?: string
  onChange?: (value: string) => void
  onBlur: (value: string) => void
  disabled?: boolean
  tabIndex?: number
  placeholder?: string
  appearAsNormalInput?: boolean
}

interface State {
  isEditing: boolean
  initialValue: string
}

@ErrorHandling
class InputClickToEdit extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    tabIndex: 0,
  }

  constructor(props) {
    super(props)

    this.state = {
      initialValue: this.props.value,
      isEditing: false,
    }

    this.handleCancel = this.handleCancel.bind(this)
    this.handleInputClick = this.handleInputClick.bind(this)
    this.handleInputBlur = this.handleInputBlur.bind(this)
    this.handleKeyDown = this.handleKeyDown.bind(this)
    this.handleChange = this.handleChange.bind(this)
    this.handleFocus = this.handleFocus.bind(this)
  }

  public handleCancel() {
    const {onChange} = this.props
    const {initialValue} = this.state
    this.setState({
      isEditing: false,
    })
    if (onChange) {
      onChange(initialValue)
    }
  }

  public handleInputClick() {
    this.setState({isEditing: true})
  }

  public handleInputBlur(e: ChangeEvent<HTMLInputElement>) {
    const {onBlur, value} = this.props
    if (value !== e.target.value) {
      onBlur(e.target.value)
    }

    this.setState({
      initialValue: e.target.value,
      isEditing: false,
    })
  }

  public handleKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    const {onBlur, value} = this.props
    if (e.key === 'Enter') {
      if (value !== e.currentTarget.value) {
        onBlur(e.currentTarget.value)
      }

      this.setState({
        initialValue: e.currentTarget.value,
        isEditing: false,
      })
    }
    if (e.key === 'Escape') {
      this.handleCancel()
    }
  }

  public handleChange(e: ChangeEvent<HTMLInputElement>) {
    const {onChange} = this.props
    if (onChange) {
      onChange(e.target.value)
    }
  }

  public handleFocus(e: ChangeEvent<HTMLInputElement>) {
    e.target.select()
  }

  public render() {
    const {isEditing} = this.state
    const {
      wrapperClass: wrapper,
      disabled,
      tabIndex,
      placeholder,
      value,
      appearAsNormalInput,
    } = this.props

    const wrapperClass = `${wrapper}${
      appearAsNormalInput ? ' input-cte__normal' : ''
    }`
    const defaultStyle = value ? 'input-cte' : 'input-cte__empty'

    return disabled ? (
      <div className={wrapperClass}>
        <div data-test="disabled" className="input-cte__disabled">
          {value}
        </div>
      </div>
    ) : (
      <div className={wrapperClass}>
        {isEditing ? (
          <input
            type="text"
            className="form-control input-sm provider--input"
            defaultValue={value}
            onBlur={this.handleInputBlur}
            onKeyDown={this.handleKeyDown}
            onChange={this.handleChange}
            autoFocus={true}
            onFocus={this.handleFocus}
            tabIndex={tabIndex}
            spellCheck={false}
          />
        ) : (
          <div
            className={defaultStyle}
            onClick={this.handleInputClick}
            onFocus={this.handleInputClick}
            tabIndex={tabIndex}
          >
            {value || placeholder}
            {appearAsNormalInput || (
              <span data-test="icon" className="icon pencil" />
            )}
          </div>
        )}
      </div>
    )
  }
}

export default InputClickToEdit
