import React, {Component, PropTypes} from 'react'

class InputClickToEdit extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
      currentValue: this.props.value,
      initialValue: this.props.value,
    }
  }

  handleCancel = () => {
    const {onKeyUpdate} = this.props
    const {initialValue} = this.state
    this.setState({
      isEditing: false,
      currentValue: initialValue,
    })
    if (onKeyUpdate) {
      onKeyUpdate(initialValue)
    }
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleInputBlur = e => {
    const {onBlurUpdate, value} = this.props
    if (value !== e.target.value) {
      onBlurUpdate(e.target.value)
    }

    this.setState({
      isEditing: false,
      currentValue: e.target.value,
      initialValue: e.target.value,
    })
  }

  handleKeyUp = e => {
    const {onKeyUpdate, value} = this.props
    if (e.key === 'Enter') {
      this.handleInputBlur(e)
      return
    }
    if (e.key === 'Escape') {
      this.handleCancel()
      return
    }
    if (onKeyUpdate && value !== e.target.value) {
      onKeyUpdate(e.target.value)
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {isEditing, currentValue: value} = this.state
    const {wrapperClass, disabled, tabIndex, placeholder} = this.props

    const divStyle = value ? 'input-cte' : 'input-cte__empty'

    return disabled
      ? <div className={wrapperClass}>
          <div className="input-cte__disabled">
            {value}
          </div>
        </div>
      : <div className={wrapperClass}>
          {isEditing
            ? <input
                type="text"
                className="form-control input-sm provider--input"
                defaultValue={value}
                onBlur={this.handleInputBlur}
                onKeyUp={this.handleKeyUp}
                autoFocus={true}
                onFocus={this.handleFocus}
                tabIndex={tabIndex}
                placeholder={placeholder}
              />
            : <div
                className={divStyle}
                onClick={this.handleInputClick}
                onFocus={this.handleInputClick}
                tabIndex={tabIndex}
              >
                {value || placeholder}
                <span className="icon pencil" />
              </div>}
        </div>
  }
}

const {func, bool, number, string} = PropTypes

InputClickToEdit.defaultValue = {
  tabIndex: 0,
}

InputClickToEdit.propTypes = {
  wrapperClass: string.isRequired,
  value: string,
  onKeyUpdate: func,
  onBlurUpdate: func.isRequired,
  disabled: bool,
  tabIndex: number,
  placeholder: string,
}

export default InputClickToEdit
