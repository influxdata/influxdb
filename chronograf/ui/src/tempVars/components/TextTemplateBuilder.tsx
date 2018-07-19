import React, {PureComponent, ChangeEvent} from 'react'

import {getDeep} from 'src/utils/wrappers'

import {TemplateValueType, TemplateBuilderProps} from 'src/types'

class TextTemplateBuilder extends PureComponent<TemplateBuilderProps> {
  public render() {
    const value = getDeep(this.props, 'template.values.0.value', '')

    return (
      <div className="form-group col-sm-12">
        <label>Text</label>
        <div className="temp-builder--mq-controls">
          <input
            className="form-control input-sm"
            value={value}
            onChange={this.handleInputChange}
          />
        </div>
      </div>
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {template, onUpdateTemplate} = this.props
    const newValue = e.target.value

    onUpdateTemplate({
      ...template,
      values: [
        {
          type: TemplateValueType.Constant,
          value: newValue,
          selected: true,
          localSelected: true,
        },
      ],
    })
  }
}

export default TextTemplateBuilder
