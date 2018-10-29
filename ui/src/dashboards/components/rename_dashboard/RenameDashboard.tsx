// Libraries
import React, {Component, KeyboardEvent, ChangeEvent} from 'react'

// Constants
import {DASHBOARD_NAME_MAX_LENGTH} from 'src/dashboards/constants/index'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onRename: (name: string) => void
  name: string
}

interface State {
  isEditing: boolean
  workingName: string
}

@ErrorHandling
class RenameDashboard extends Component<Props, State> {
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
      return <div className="rename-dashboard">{this.input}</div>
    }

    return (
      <div className="rename-dashboard">
        <div
          className="rename-dashboard--title"
          onClick={this.handleStartEditing}
        >
          {name}
          <span className="icon pencil" />
        </div>
      </div>
    )
  }

  private get input(): JSX.Element {
    const {workingName} = this.state

    return (
      <input
        maxLength={DASHBOARD_NAME_MAX_LENGTH}
        type="text"
        value={workingName}
        autoFocus={true}
        spellCheck={false}
        placeholder="Name this Dashboard"
        onFocus={this.handleInputFocus}
        onBlur={this.handleInputBlur(false)}
        onChange={this.handleInputChange}
        onKeyDown={this.handleKeyDown}
        className="rename-dashboard--input"
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingName: e.target.value})
  }

  private handleInputBlur = (reset: boolean) => (): void => {
    const {onRename} = this.props

    if (reset) {
      onRename(this.props.name)
    } else {
      onRename(this.state.workingName)
    }

    this.setState({isEditing: false})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      this.handleInputBlur(false)()
    }

    if (e.key === 'Escape') {
      this.handleInputBlur(true)()
    }
  }

  private handleInputFocus = (e: ChangeEvent<HTMLInputElement>): void => {
    e.currentTarget.select()
  }
}

export default RenameDashboard
