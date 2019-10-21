// Libraries
import React, {
  PureComponent,
  KeyboardEvent,
  ChangeEvent,
  MouseEvent,
} from 'react'
import classnames from 'classnames'

// Components
import {
  Input,
  Icon,
  IconFont,
  Page,
  FlexBox,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onRename: (name: string) => void
  onClickOutside?: (e: MouseEvent<HTMLElement>) => void
  name: string
  placeholder: string
  maxLength: number
  prefix: string
}

interface State {
  isEditing: boolean
  workingName: string
}

@ErrorHandling
class RenamablePageTitle extends PureComponent<Props, State> {
  public static defaultProps = {
    prefix: '',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      workingName: props.name,
    }
  }

  public render() {
    const {name, placeholder} = this.props
    const {isEditing} = this.state

    let title = (
      <div className={this.titleClassName} onClick={this.handleStartEditing}>
        <Page.Title title={name || placeholder} />
        <Icon glyph={IconFont.Pencil} />
      </div>
    )

    if (isEditing) {
      title = (
        <ClickOutside onClickOutside={this.handleStopEditing}>
          {this.input}
        </ClickOutside>
      )
    }

    return (
      <FlexBox
        direction={FlexDirection.Column}
        alignItems={AlignItems.FlexStart}
        className="renamable-page-title"
      >
        {title}
        {this.prefix}
      </FlexBox>
    )
  }

  private get prefix(): JSX.Element {
    const {prefix} = this.props
    if (prefix) {
      return <Page.SubTitle title={prefix} />
    }
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
        className="renamable-page-title--input"
        value={workingName}
      />
    )
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = e => {
    const {workingName} = this.state
    const {onRename, onClickOutside} = this.props

    onRename(workingName)

    if (onClickOutside) {
      onClickOutside(e)
    }

    this.setState({isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingName: e.target.value})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const {onRename, name} = this.props
    const {workingName} = this.state

    if (e.key === 'Enter') {
      onRename(workingName)
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
