// Libraries
import React, {Component, KeyboardEvent, ChangeEvent, MouseEvent} from 'react'
import classnames from 'classnames'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Components
import {Input, Icon} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {ComponentSize, IconFont} from '@influxdata/clockface'
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onUpdate: (name: string) => void
  name: string
  noNameString: string
  hrefValue: string
  testID: string
  onEditName?: (e?: MouseEvent<HTMLAnchorElement>) => void
  placeholder?: string
}

interface State {
  isEditing: boolean
  workingName: string
  loading: RemoteDataState
}

@ErrorHandling
class EditableName extends Component<Props, State> {
  public static defaultProps = {
    hrefValue: 'javascript:void(0);',
    testID: 'editable-name',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      isEditing: false,
      workingName: props.name,
      loading: RemoteDataState.Done,
    }
  }

  public render() {
    const {name, onEditName, hrefValue, noNameString, testID} = this.props

    return (
      <div className={this.className} data-testid={testID}>
        <SpinnerContainer
          loading={this.state.loading}
          spinnerComponent={<TechnoSpinner diameterPixels={20} />}
        >
          <a href={hrefValue} onClick={onEditName}>
            <span>{name || noNameString}</span>
          </a>
        </SpinnerContainer>
        <div
          className="editable-name--toggle"
          onClick={this.handleStartEditing}
          data-testid={testID + '--toggle'}
        >
          <Icon glyph={IconFont.Pencil} />
        </div>
        {this.input}
      </div>
    )
  }

  private get input(): JSX.Element {
    const {placeholder} = this.props
    const {workingName, isEditing, loading} = this.state

    if (isEditing && loading !== RemoteDataState.Loading) {
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
            className="editable-name--input"
            value={workingName}
          />
        </ClickOutside>
      )
    }
  }

  private handleStartEditing = (): void => {
    this.setState({isEditing: true})
  }

  private handleStopEditing = () => {
    const {workingName} = this.state
    const {onUpdate} = this.props

    this.setState({loading: RemoteDataState.Loading})
    onUpdate(workingName)
    this.setState({loading: RemoteDataState.Done, isEditing: false})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({workingName: e.target.value})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const {onUpdate, name} = this.props
    const {workingName} = this.state

    if (e.key === 'Enter') {
      e.persist()
      if (!workingName) {
        this.setState({isEditing: false, workingName: name})
        return
      }
      this.setState({loading: RemoteDataState.Loading})
      onUpdate(workingName)
      this.setState({isEditing: false, loading: RemoteDataState.Done})
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
