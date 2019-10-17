// Libraries
import React, {Component, KeyboardEvent, ChangeEvent} from 'react'
import classnames from 'classnames'

// Components
import {Input, Icon} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {ComponentSize, IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onUpdate: (name: string) => void
  description: string
  placeholder?: string
}

interface State {
  isEditing: boolean
  workingDescription: string
}

@ErrorHandling
class EditableDescription extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      workingDescription: props.description,
    }
  }

  public render() {
    const {description} = this.props
    const {isEditing} = this.state

    if (isEditing) {
      return (
        <div className="editable-description">
          <ClickOutside onClickOutside={this.handleStopEditing}>
            {this.input}
          </ClickOutside>
        </div>
      )
    }

    return (
      <div className="editable-description">
        <div
          className={this.previewClassName}
          onClick={this.handleStartEditing}
        >
          {description || 'No description'}
          <Icon glyph={IconFont.Pencil} />
        </div>
      </div>
    )
  }

  private get input(): JSX.Element {
    const {placeholder} = this.props
    const {workingDescription} = this.state

    return (
      <Input
        size={ComponentSize.ExtraSmall}
        maxLength={90}
        autoFocus={true}
        spellCheck={false}
        placeholder={placeholder}
        onFocus={this.handleInputFocus}
        onChange={this.handleInputChange}
        onKeyDown={this.handleKeyDown}
        className="editable-description--input"
        value={workingDescription}
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = () => {
    const {workingDescription} = this.state
    const {onUpdate} = this.props

    onUpdate(workingDescription)
    this.setState({isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingDescription: e.target.value})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const {onUpdate, description} = this.props
    const {workingDescription} = this.state

    if (e.key === 'Enter') {
      onUpdate(workingDescription)
      this.setState({isEditing: false})
    }

    if (e.key === 'Escape') {
      this.setState({isEditing: false, workingDescription: description})
    }
  }

  private handleInputFocus = (e: ChangeEvent<HTMLInputElement>): void => {
    e.currentTarget.select()
  }

  private get previewClassName(): string {
    const {description} = this.props

    return classnames('editable-description--preview', {
      untitled: !description,
    })
  }
}

export default EditableDescription
