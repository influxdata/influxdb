import React, {Component, PropTypes} from 'react'

class InputClickToEdit extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
      initialValue: this.props.value,
    }
  }

  handleCancel = () => {
    const {onChange} = this.props
    const {initialValue} = this.state
    this.setState({
      isEditing: false,
    })
    if (onChange) {
      onChange(initialValue)
    }
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleInputBlur = e => {
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
    onChange(e.target.value)
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {isEditing} = this.state
    const {wrapperClass, disabled, tabIndex, placeholder, value} = this.props

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
                value={value || ''}
                onBlur={this.handleInputBlur}
                onKeyDown={this.handleKeyDown}
                onChange={this.handleChange}
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
  onChange: func,
  onBlur: func.isRequired,
  disabled: bool,
  tabIndex: number,
  placeholder: string,
}

export default InputClickToEdit
