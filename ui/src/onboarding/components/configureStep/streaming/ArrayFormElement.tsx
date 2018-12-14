// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form} from 'src/clockface'
import TagInput, {Item} from 'src/shared/components/TagInput'

interface Props {
  fieldName: string
  addTagValue: (item: string, fieldName: string) => void
  removeTagValue: (item: string, fieldName: string) => void
  autoFocus: boolean
  value: string[]
  helpText: string
}

class ConfigFieldSwitcher extends PureComponent<Props> {
  public render() {
    const {fieldName, autoFocus, helpText} = this.props

    return (
      <Form.Element label={fieldName} key={fieldName} helpText={helpText}>
        <TagInput
          title={fieldName}
          autoFocus={autoFocus}
          displayTitle={false}
          onAddTag={this.handleAddTag}
          onDeleteTag={this.handleRemoveTag}
          tags={this.tags}
        />
      </Form.Element>
    )
  }

  private handleAddTag = (item: string) => {
    this.props.addTagValue(item, this.props.fieldName)
  }

  private handleRemoveTag = (item: string) => {
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

export default ConfigFieldSwitcher
