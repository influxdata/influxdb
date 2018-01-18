import React, {Component, PropTypes} from 'react'

class InputClickToEdit extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
      isEditing: null,
      inputValue: this.props.value,
    }
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleInputBlur = reset => e => {
    const {onConfirm, value} = this.props

    if (!reset && value !== e.target.value) {
      onConfirm(e.target.value)
    }

    this.setState({reset: false, isEditing: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.setState({reset: true, value: this.props.value}, () =>
        this.inputRef.blur()
      )
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  // handleInputChange = e => {
  //   this.setState({inputValue: e.target.value})
  // }

  render() {
    const {reset, isEditing, inputValue} = this.state
    const {wrapperClass, disabled} = this.props

    return disabled
      ? <div className={wrapperClass}>
          <div className="input-cte__disabled">
            {inputValue}
          </div>
        </div>
      : <div className={wrapperClass}>
          {isEditing
            ? <input
                type="text"
                className="form-control input-sm provider--input"
                defaultValue={inputValue}
                onBlur={this.handleInputBlur(reset)}
                onKeyDown={this.handleKeyDown}
                autoFocus={true}
                onFocus={this.handleFocus}
                ref={r => (this.inputRef = r)}
              />
            : <div className="input-cte" onClick={this.handleInputClick}>
                {inputValue}
                <span className="icon pencil" />
              </div>}
        </div>
  }
}

const {func, bool, string} = PropTypes

InputClickToEdit.propTypes = {
  wrapperClass: string.isRequired,
  value: string,
  onConfirm: func.isRequired,
  disabled: bool,
}

export default InputClickToEdit
