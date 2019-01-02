// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import Rows from 'src/onboarding/components/configureStep/streaming/MultipleRows'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Input,
  InputType,
  AutoComplete,
  ComponentStatus,
  ComponentSize,
  Grid,
  Columns,
  Form,
} from 'src/clockface'

// Utils
import {validateURI} from 'src/shared/utils/validateURI'

// Actions
import {setConfigArrayValue} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPluginName, ConfigFieldType} from 'src/types/v2/dataLoaders'

const VALIDATE_DEBOUNCE_MS = 350

export interface Item {
  text?: string
  name?: string
}
interface Props {
  onAddRow: (item: string) => void
  onDeleteRow: (item: string) => void
  tags: Item[]
  title: string
  helpText: string
  inputID?: string
  fieldType?: ConfigFieldType
  autoFocus?: boolean
  onSetConfigArrayValue: typeof setConfigArrayValue
  telegrafPluginName: TelegrafPluginName
}

interface State {
  editingText: string
  status: ComponentStatus
}

@ErrorHandling
class MultipleInput extends PureComponent<Props, State> {
  private debouncedValidate: (value: string) => void

  constructor(props: Props) {
    super(props)
    this.state = {editingText: '', status: ComponentStatus.Default}

    this.debouncedValidate = _.debounce(
      this.handleValidateURI,
      VALIDATE_DEBOUNCE_MS
    )
  }

  public render() {
    const {
      title,
      helpText,
      tags,
      autoFocus,
      onSetConfigArrayValue,
      telegrafPluginName,
    } = this.props
    const {editingText} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Ten} offsetXS={Columns.One}>
            <Form.Element label={title} key={title} helpText={helpText}>
              <Input
                placeholder={`Type and hit 'Enter' to add to list of ${title}`}
                autocomplete={AutoComplete.Off}
                type={InputType.Text}
                onKeyDown={this.handleKeyDown}
                autoFocus={autoFocus || false}
                value={editingText}
                status={this.state.status}
                onChange={this.handleInputChange}
                size={ComponentSize.Medium}
                titleText={title}
              />
            </Form.Element>
            <Rows
              tags={tags}
              onDeleteTag={this.handleDeleteRow}
              onSetConfigArrayValue={onSetConfigArrayValue}
              fieldName={title}
              telegrafPluginName={telegrafPluginName}
            />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {fieldType} = this.props
    const {value} = e.target

    this.setState({editingText: value})
    if (fieldType === ConfigFieldType.UriArray) {
      this.debouncedValidate(value)
    }
  }

  private handleKeyDown = e => {
    if (e.key === 'Enter') {
      e.preventDefault()
      const newItem = e.target.value.trim()
      const {tags, onAddRow} = this.props
      if (!this.shouldAddToList(newItem, tags)) {
        return
      }
      this.setState({editingText: ''})
      onAddRow(e.target.value)
    }
  }

  private handleDeleteRow = (item: Item) => {
    this.props.onDeleteRow(item.name || item.text)
  }

  private shouldAddToList(item: Item, tags: Item[]): boolean {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }

  private handleValidateURI = (value: string): void => {
    if (validateURI(value)) {
      this.setState({status: ComponentStatus.Valid})
    } else {
      this.setState({status: ComponentStatus.Error})
    }
  }
}

export default MultipleInput
