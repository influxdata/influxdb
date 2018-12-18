// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form} from 'src/clockface'
import MultipleInput, {Item} from './MultipleInput'

// Actions
import {setConfigArrayValue} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPluginName} from 'src/types/v2/dataLoaders'

interface Props {
  fieldName: string
  addTagValue: (item: string, fieldName: string) => void
  removeTagValue: (item: string, fieldName: string) => void
  autoFocus: boolean
  value: string[]
  helpText: string
  onSetConfigArrayValue: typeof setConfigArrayValue
  telegrafPluginName: TelegrafPluginName
}

class ArrayFormElement extends PureComponent<Props> {
  public render() {
    const {
      fieldName,
      autoFocus,
      helpText,
      onSetConfigArrayValue,
      telegrafPluginName,
    } = this.props
    return (
      <div className="multiple-input-index">
        <Form.Element label={fieldName} key={fieldName} helpText={helpText}>
          <MultipleInput
            title={fieldName}
            autoFocus={autoFocus}
            displayTitle={false}
            onAddRow={this.handleAddRow}
            onDeleteRow={this.handleRemoveRow}
            tags={this.tags}
            onSetConfigArrayValue={onSetConfigArrayValue}
            telegrafPluginName={telegrafPluginName}
          />
        </Form.Element>
      </div>
    )
  }

  private handleAddRow = (item: string) => {
    this.props.addTagValue(item, this.props.fieldName)
  }

  private handleRemoveRow = (item: string) => {
    const {removeTagValue, fieldName} = this.props

    removeTagValue(item, fieldName)
  }

  private get tags(): Item[] {
    const {value} = this.props
    return value.map(v => {
      return {text: v, name: v}
    })
  }
}

export default ArrayFormElement
