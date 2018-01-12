import React, {Component, PropTypes} from 'react'

class InputClickToEdit extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: null,
      value: this.props.value,
    }
  }

  handleCancel = () => {
    this.setState({
      isEditing: false,
      value: this.props.value,
    })
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleInputBlur = e => {
    const {onUpdate, value} = this.props

    if (value !== e.target.value) {
      onUpdate(e.target.value)
    }

    this.setState({isEditing: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.handleInputBlur(e)
    }
    if (e.key === 'Escape') {
      this.handleCancel()
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {isEditing, value} = this.state
    const {wrapperClass, disabled} = this.props

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
                onKeyDown={this.handleKeyDown}
                autoFocus={true}
                onFocus={this.handleFocus}
                ref={r => (this.inputRef = r)}
              />
            : <div className="input-cte" onClick={this.handleInputClick}>
                {value}
                <span className="icon pencil" />
              </div>}
        </div>
  }
}

const {func, bool, string} = PropTypes

InputClickToEdit.propTypes = {
  wrapperClass: string.isRequired,
  value: string,
  onUpdate: func.isRequired,
  disabled: bool,
}

export default InputClickToEdit
