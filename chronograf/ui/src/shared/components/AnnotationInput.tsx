import React, {Component, ChangeEvent, FocusEvent, KeyboardEvent} from 'react'
import onClickOutside from 'react-onclickoutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  isEditing: boolean
}

interface Props {
  value: string
  onChangeInput: (i: string) => void
  onConfirmUpdate: () => void
  onRejectUpdate: () => void
}

@ErrorHandling
class AnnotationInput extends Component<Props, State> {
  public state = {
    isEditing: false,
  }

  public render() {
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
  public handleClickOutside = () => {
    this.props.onConfirmUpdate()
    this.setState({isEditing: false})
  }

  private handleInputClick = () => {
    this.setState({isEditing: true})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
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

  private handleFocus = (e: FocusEvent<HTMLInputElement>) => {
    e.target.select()
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onChangeInput(e.target.value)
  }
}

export default onClickOutside(AnnotationInput)
