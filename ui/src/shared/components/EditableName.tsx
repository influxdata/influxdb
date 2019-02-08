// Libraries
import React, {Component, KeyboardEvent, ChangeEvent, MouseEvent} from 'react'
import classnames from 'classnames'

// Components
import {Input, ComponentSize} from 'src/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import 'src/shared/components/EditableName.scss'

interface PassedProps {
  onUpdate: (name: string) => void
  name: string
  onEditName?: (e?: MouseEvent<HTMLAnchorElement>) => void
  placeholder?: string
  noNameString: string
}

interface DefaultProps {
  hrefValue?: string
}

type Props = PassedProps & DefaultProps

interface State {
  isEditing: boolean
  workingName: string
}

@ErrorHandling
class EditableName extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    hrefValue: '#',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      workingName: props.name,
    }
  }

  public render() {
    const {name, onEditName, hrefValue, noNameString} = this.props

    return (
      <div className={this.className}>
        <a href={hrefValue} onClick={onEditName}>
          <span>{name || noNameString}</span>
        </a>
        <div
          className="editable-name--toggle"
          onClick={this.handleStartEditing}
        >
          <span className="icon pencil" />
        </div>
        {this.input}
      </div>
    )
  }

  private get input(): JSX.Element {
    const {placeholder} = this.props
    const {workingName, isEditing} = this.state

    if (isEditing) {
      return (
        <ClickOutside onClickOutside={this.handleStopEditing}>
          <Input
            size={ComponentSize.ExtraSmall}
            maxLength={90}
            autoFocus={true}
            spellCheck={false}
            placeholder={placeholder}
            onFocus={this.handleInputFocus}
            onChange={this.handleInputChange}
            onKeyDown={this.handleKeyDown}
            customClass="editable-name--input"
            value={workingName}
          />
        </ClickOutside>
      )
    }
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = async (): Promise<void> => {
    const {workingName} = this.state
    const {onUpdate} = this.props

    await onUpdate(workingName)

    this.setState({isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingName: e.target.value})
  }

  private handleKeyDown = async (
    e: KeyboardEvent<HTMLInputElement>
  ): Promise<void> => {
    const {onUpdate, name} = this.props
    const {workingName} = this.state

    if (e.key === 'Enter') {
      await onUpdate(workingName)
      this.setState({isEditing: false})
    }

    if (e.key === 'Escape') {
      this.setState({isEditing: false, workingName: name})
    }
  }

  private handleInputFocus = (e: ChangeEvent<HTMLInputElement>): void => {
    e.currentTarget.select()
  }

  private get className(): string {
    const {name, noNameString} = this.props

    return classnames('editable-name', {
      'untitled-name': name === noNameString,
    })
  }
}

export default EditableName
