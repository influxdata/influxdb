// Libraries
import React, {Component, KeyboardEvent, ChangeEvent, MouseEvent} from 'react'
import classnames from 'classnames'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Components
import {Input, ComponentSize} from 'src/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import 'src/clockface/components/resource_list/ResourceName.scss'

interface PassedProps {
  onUpdate: (name: string) => void
  name: string
  onEditName?: (e?: MouseEvent<HTMLAnchorElement>) => void
  placeholder?: string
  noNameString: string
}

interface DefaultProps {
  parentTestID?: string
  buttonTestID?: string
  inputTestID?: string
  hrefValue?: string
  onClick?: () => void
}

type Props = PassedProps & DefaultProps

interface State {
  isEditing: boolean
  workingName: string
  loading: RemoteDataState
}

@ErrorHandling
class ResourceName extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    parentTestID: 'resource-name',
    buttonTestID: 'resource-name--button',
    inputTestID: 'resource-name--input',
    hrefValue: '#',
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
    const {
      name,
      onEditName,
      hrefValue,
      noNameString,
      parentTestID,
      buttonTestID,
    } = this.props

    return (
      <div className={this.className} data-testid={parentTestID}>
        <SpinnerContainer
          loading={this.state.loading}
          spinnerComponent={<TechnoSpinner diameterPixels={20} />}
        >
          <a href={hrefValue} onClick={onEditName}>
            <span>{name || noNameString}</span>
          </a>
        </SpinnerContainer>
        <div
          className="resource-name--toggle"
          onClick={this.handleStartEditing}
          data-testid={buttonTestID}
        >
          <span className="icon pencil" />
        </div>
        {this.input}
      </div>
    )
  }

  private get input(): JSX.Element {
    const {placeholder, inputTestID} = this.props
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
            customClass="resource-name--input"
            value={workingName}
            testID={inputTestID}
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

    this.setState({loading: RemoteDataState.Loading})
    await onUpdate(workingName)
    this.setState({loading: RemoteDataState.Done, isEditing: false})
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
      e.persist()

      if (!workingName) {
        this.setState({isEditing: false, workingName: name})

        return
      }
      this.setState({loading: RemoteDataState.Loading})
      await onUpdate(workingName)
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
    const {isEditing} = this.state

    return classnames('resource-name', {
      'resource-name--editing': isEditing,
      'untitled-name': name === noNameString,
    })
  }
}

export default ResourceName
