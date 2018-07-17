import React, {Component, KeyboardEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import './RenameDashboard.scss'
import {
  DASHBOARD_NAME_MAX_LENGTH,
  DEFAULT_DASHBOARD_NAME,
} from 'src/dashboards/constants/index'

interface Props {
  onRename: (name: string) => void
  name: string
}

interface State {
  isEditing: boolean
  reset: boolean
}
@ErrorHandling
class RenameDashboard extends Component<Props, State> {
  private inputRef: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      reset: false,
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
    const {name} = this.props

    return (
      <input
        maxLength={DASHBOARD_NAME_MAX_LENGTH}
        type="text"
        className="rename-dashboard--input form-control input-sm"
        defaultValue={name}
        autoComplete="off"
        autoFocus={true}
        spellCheck={false}
        onBlur={this.handleInputBlur}
        onKeyDown={this.handleKeyDown}
        onFocus={this.handleFocus}
        placeholder="Name this Dashboard"
        ref={r => (this.inputRef = r)}
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleInputBlur = e => {
    const {onRename} = this.props
    const {reset} = this.state

    if (reset) {
      this.setState({isEditing: false})
    } else {
      const newName = e.target.value || DEFAULT_DASHBOARD_NAME
      onRename(newName)
    }
    this.setState({isEditing: false, reset: false})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.inputRef.value = this.props.name
      this.setState({reset: true}, () => this.inputRef.blur())
    }
  }

  private handleFocus = (e): void => {
    e.target.select()
  }
}

export default RenameDashboard
