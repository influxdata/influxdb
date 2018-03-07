import React, {PureComponent} from 'react'

interface Props {
  wrapperClass: string
  value?: string
  onChange?: (value: string) => void
  onBlur: (value: string) => void
  disabled?: boolean
  tabIndex?: number
  placeholder?: string
  appearAsNormalInput: boolean
}

interface State {
  isEditing: boolean
  initialValue: string
}

class InputClickToEdit extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
      initialValue: this.props.value,
    }

    this.handleCancel = this.handleCancel.bind(this)
    this.handleInputClick = this.handleInputClick.bind(this)
    this.handleInputBlur = this.handleInputBlur.bind(this)
    this.handleKeyDown = this.handleKeyDown.bind(this)
    this.handleChange = this.handleChange.bind(this)
    this.handleFocus = this.handleFocus.bind(this)
  }

  public static defaultProps: Partial<Props> = {
    tabIndex: 0,
  }

  handleCancel() {
    const {onChange} = this.props
    const {initialValue} = this.state
    this.setState({
      isEditing: false,
    })
    if (onChange) {
      onChange(initialValue)
    }
  }

  handleInputClick() {
    this.setState({isEditing: true})
  }

  handleInputBlur(e) {
    const {onBlur, value} = this.props
    if (value !== e.target.value) {
      onBlur(e.target.value)
    }

    this.setState({
      isEditing: false,
      initialValue: e.target.value,
    })
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.handleInputBlur(e)
    }
    if (e.key === 'Escape') {
      this.handleCancel()
    }
  }

  handleChange = e => {
    const {onChange} = this.props
    if (onChange) {
      onChange(e.target.value)
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {isEditing} = this.state
    const {
      wrapperClass: wrapper,
      disabled,
      tabIndex,
      placeholder,
      value,
      appearAsNormalInput,
    } = this.props

    const wrapperClass = `${wrapper}${appearAsNormalInput
      ? ' input-cte__normal'
      : ''}`
    const defaultStyle = value ? 'input-cte' : 'input-cte__empty'

    return disabled
      ? <div className={wrapperClass}>
          <div data-test="disabled" className="input-cte__disabled">
            {value}
          </div>
        </div>
      : <div className={wrapperClass}>
          {isEditing
            ? <input
                data-test="input"
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
            : <div
                data-test="unclicked"
                className={defaultStyle}
                onClick={this.handleInputClick}
                onFocus={this.handleInputClick}
                tabIndex={tabIndex}
              >
                {value || placeholder}
                {appearAsNormalInput ||
                  <span data-test="icon" className="icon pencil" />}
              </div>}
        </div>
  }
}

export default InputClickToEdit
