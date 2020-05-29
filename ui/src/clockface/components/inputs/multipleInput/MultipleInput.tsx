// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {Button, Form, Input, Grid} from '@influxdata/clockface'
import Rows from 'src/clockface/components/inputs/multipleInput/MultipleRows'

// Utils
import {validateURI} from 'src/shared/utils/validateURI'

// Types
import {
  AutoComplete,
  Columns,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  InputType,
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

const VALIDATE_DEBOUNCE_MS = 350

export interface Item {
  text?: string
  name?: string
}

export enum MultiInputType {
  String = 'string',
  URI = 'uri',
}

interface Props {
  onAddRow: (item: string) => void
  onDeleteRow: (item: string) => void
  onEditRow: (index: number, item: string) => void
  tags: Item[]
  title: string
  helpText: string
  inputID?: string
  inputType?: MultiInputType
  autoFocus?: boolean
}

interface State {
  editingText: string
  status: ComponentStatus
}

@ErrorHandling
class MultipleInput extends PureComponent<Props, State> {
  private debouncedValidate: (value: string) => void
  private inputRef

  constructor(props: Props) {
    super(props)
    this.state = {editingText: '', status: ComponentStatus.Default}

    this.inputRef = React.createRef()

    this.debouncedValidate = _.debounce(
      this.handleValidateURI,
      VALIDATE_DEBOUNCE_MS
    )
  }

  public render() {
    const {title, helpText, tags, autoFocus, onEditRow} = this.props
    const {editingText} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.One}>
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
                ref={this.inputRef}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Two}>
            <Form.Element
              key="submit"
              label="&nbsp;"
              style={{minHeight: '15px'}}
            >
              <Button
                color={ComponentColor.Secondary}
                onClick={this.handleSubmit}
                size={ComponentSize.Medium}
                text="Add"
                titleText={`Add to list of ${title}`}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Ten} offsetXS={Columns.One}>
            <Rows
              tags={tags}
              onDeleteTag={this.handleDeleteRow}
              onChange={onEditRow}
            />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {inputType} = this.props
    const {value} = e.target

    this.setState({editingText: value})
    if (inputType === MultiInputType.URI) {
      this.debouncedValidate(value)
    }
  }

  private handleSubmit = e => {
    e.preventDefault()
    const newItem = this.inputRef.current.value.trim()
    const {tags, onAddRow} = this.props
    if (!this.shouldAddToList(newItem, tags)) {
      return
    }
    this.setState({editingText: ''})
    onAddRow(this.inputRef.current.value)
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
