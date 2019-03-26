// Libraries
import React, {Component, KeyboardEvent, ChangeEvent} from 'react'
import classnames from 'classnames'

// Components
import {Input} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onRename: (name: string) => void
  name: string
  placeholder: string
  maxLength: number
}

interface State {
  isEditing: boolean
  workingName: string
}

@ErrorHandling
class RenamablePageTitle extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      workingName: props.name,
    }
  }

  public render() {
    const {isEditing} = this.state
    const {name, placeholder} = this.props

    if (isEditing) {
      return (
        <div className="renamable-page-title">
          <ClickOutside onClickOutside={this.handleStopEditing}>
            {this.input}
          </ClickOutside>
        </div>
      )
    }

    return (
      <div className="renamable-page-title">
        <div className={this.titleClassName} onClick={this.handleStartEditing}>
          {name || placeholder}
          <span className="icon pencil" />
        </div>
      </div>
    )
  }

  private get input(): JSX.Element {
    const {placeholder, maxLength} = this.props
    const {workingName} = this.state

    return (
      <Input
        maxLength={maxLength}
        autoFocus={true}
        spellCheck={false}
        placeholder={placeholder}
        onFocus={this.handleInputFocus}
        onChange={this.handleInputChange}
        onKeyDown={this.handleKeyDown}
        customClass="renamable-page-title--input"
        value={workingName}
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = async (): Promise<void> => {
    const {workingName} = this.state
    const {onRename} = this.props

    await onRename(workingName)

    this.setState({isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingName: e.target.value})
  }

  private handleKeyDown = async (
    e: KeyboardEvent<HTMLInputElement>
  ): Promise<void> => {
    const {onRename, name} = this.props
    const {workingName} = this.state

    if (e.key === 'Enter') {
      await onRename(workingName)
      this.setState({isEditing: false})
    }

    if (e.key === 'Escape') {
      this.setState({isEditing: false, workingName: name})
    }
  }

  private handleInputFocus = (e: ChangeEvent<HTMLInputElement>): void => {
    e.currentTarget.select()
  }

  private get titleClassName(): string {
    const {name, placeholder} = this.props

    const nameIsUntitled = name === placeholder || name === ''

    return classnames('renamable-page-title--title', {
      untitled: nameIsUntitled,
    })
  }
}

export default RenamablePageTitle
