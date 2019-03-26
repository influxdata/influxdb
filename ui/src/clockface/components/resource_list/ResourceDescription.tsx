// Libraries
import React, {Component, KeyboardEvent, ChangeEvent} from 'react'
import classnames from 'classnames'

// Components
import {Input} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {ComponentSize} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import 'src/clockface/components/resource_list/ResourceDescription.scss'

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
class ResourceDescription extends Component<Props, State> {
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
        <div className="resource-description">
          <ClickOutside onClickOutside={this.handleStopEditing}>
            {this.input}
          </ClickOutside>
        </div>
      )
    }

    return (
      <div className="resource-description">
        <div
          className={this.previewClassName}
          onClick={this.handleStartEditing}
        >
          {description || 'No description'}
          <span className="icon pencil" />
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
        autoFocus={true}
        spellCheck={false}
        placeholder={placeholder}
        onFocus={this.handleInputFocus}
        onChange={this.handleInputChange}
        onKeyDown={this.handleKeyDown}
        customClass="resource-description--input"
        value={workingDescription}
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = async (): Promise<void> => {
    const {workingDescription} = this.state
    const {onUpdate} = this.props

    await onUpdate(workingDescription)

    this.setState({isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingDescription: e.target.value})
  }

  private handleKeyDown = async (
    e: KeyboardEvent<HTMLInputElement>
  ): Promise<void> => {
    const {onUpdate, description} = this.props
    const {workingDescription} = this.state

    if (e.key === 'Enter') {
      await onUpdate(workingDescription)
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

    return classnames('resource-description--preview', {
      untitled: !description,
    })
  }
}

export default ResourceDescription
