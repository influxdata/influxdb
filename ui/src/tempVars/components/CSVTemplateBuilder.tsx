import React, {PureComponent, ChangeEvent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'
import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'

import {TemplateBuilderProps, TemplateValueType} from 'src/types'

interface State {
  templateValues: string[]
  templateValuesString: string
}

@ErrorHandling
class CSVTemplateBuilder extends PureComponent<TemplateBuilderProps, State> {
  public constructor(props) {
    super(props)

    const templateValues = props.template.values.map(v => v.value)

    this.state = {
      templateValues,
      templateValuesString: templateValues.join(', '),
    }
  }

  public render() {
    const {templateValues, templateValuesString} = this.state
    const pluralizer = templateValues.length === 1 ? '' : 's'

    return (
      <div className="temp-builder csv-temp-builder">
        <div className="form-group">
          <label>Comma Separated Values</label>
          <div className="temp-builder--mq-controls">
            <textarea
              className="form-control"
              value={templateValuesString}
              onChange={this.handleChange}
              onBlur={this.handleBlur}
            />
          </div>
        </div>
        <div className="temp-builder-results">
          <p>
            CSV contains <strong>{templateValues.length}</strong> value{
              pluralizer
            }
          </p>
          {templateValues.length > 0 && (
            <TemplatePreviewList items={templateValues} />
          )}
        </div>
      </div>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    this.setState({templateValuesString: e.target.value})
  }

  private handleBlur = (): void => {
    const {template, onUpdateTemplate} = this.props
    const {templateValuesString} = this.state

    let templateValues

    if (templateValuesString.trim() === '') {
      templateValues = []
    } else {
      templateValues = templateValuesString.split(',').map(s => s.trim())
    }

    this.setState({templateValues})

    const nextValues = templateValues.map(value => {
      return {
        type: TemplateValueType.CSV,
        value,
        selected: false,
      }
    })

    if (nextValues.length > 0) {
      nextValues[0].selected = true
    }

    onUpdateTemplate({...template, values: nextValues})
  }
}

export default CSVTemplateBuilder
