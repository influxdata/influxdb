import React, {Component} from 'react'
import PropTypes from 'prop-types'
import onClickOutside from 'react-onclickoutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class AnnotationInput extends Component {
  state = {
    isEditing: false,
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleKeyDown = e => {
    const {onConfirmUpdate, onRejectUpdate} = this.props

    if (e.key === 'Enter') {
      onConfirmUpdate()
      this.setState({isEditing: false})
    }
    if (e.key === 'Escape') {
      onRejectUpdate()
      this.setState({isEditing: false})
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  handleChange = e => {
    this.props.onChangeInput(e.target.value)
  }

  handleClickOutside = () => {
    this.props.onConfirmUpdate()
    this.setState({isEditing: false})
  }

  render() {
    const {isEditing} = this.state
    const {value} = this.props

    return (
      <div className="annotation-tooltip--input-container">
        {isEditing ? (
          <input
            type="text"
            className="annotation-tooltip--input form-control input-xs"
            value={value}
            onChange={this.handleChange}
            onKeyDown={this.handleKeyDown}
            autoFocus={true}
            onFocus={this.handleFocus}
            placeholder="Annotation text"
          />
        ) : (
          <div className="input-cte" onClick={this.handleInputClick}>
            {value}
            <span className="icon pencil" />
          </div>
        )}
      </div>
    )
  }
}

const {func, string} = PropTypes

AnnotationInput.propTypes = {
  value: string,
  onChangeInput: func.isRequired,
  onConfirmUpdate: func.isRequired,
  onRejectUpdate: func.isRequired,
}

export default onClickOutside(AnnotationInput)
