// Libraries
import React, {Component, KeyboardEvent, ChangeEvent} from 'react'
import classnames from 'classnames'

// Components
import {Input} from 'src/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Constants
import {
  DASHBOARD_NAME_MAX_LENGTH,
  DEFAULT_DASHBOARD_NAME,
} from 'src/dashboards/constants/index'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import 'src/pageLayout/components/RenamablePageTitle.scss'

interface Props {
  onRename: (name: string) => void
  name: string
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
    const {name} = this.props

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
          {name || DEFAULT_DASHBOARD_NAME}
          <span className="icon pencil" />
        </div>
      </div>
    )
  }

  private get input(): JSX.Element {
    const {workingName} = this.state

    return (
      <Input
        maxLength={DASHBOARD_NAME_MAX_LENGTH}
        autoFocus={true}
        spellCheck={false}
        placeholder={DEFAULT_DASHBOARD_NAME}
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
    const {name} = this.props

    const nameIsUntitled = name === DEFAULT_DASHBOARD_NAME || name === ''

    return classnames('renamable-page-title--title', {
      untitled: nameIsUntitled,
    })
  }
}

export default RenamablePageTitle
