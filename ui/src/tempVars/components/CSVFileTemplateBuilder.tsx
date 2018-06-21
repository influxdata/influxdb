import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {notifyCSVUploadFailed} from 'src/shared/copy/notifications'

import {TemplateBuilderProps, TemplateValueType, TemplateValue} from 'src/types'

interface State {
  templateValues: string[]
  templateValuesString: string
}

@ErrorHandling
class CSVFileTemplateBuilder extends PureComponent<
  TemplateBuilderProps,
  State
> {
  public constructor(props: TemplateBuilderProps) {
    super(props)
    const templateValues = props.template.values.map(v => v.value)

    this.state = {
      templateValues,
      templateValuesString: templateValues.join(', '),
    }
  }

  public render() {
    const {templateValues} = this.state
    const pluralizer = templateValues.length === 1 ? '' : 's'
    return (
      <>
        <DragAndDrop
          submitText="Preview"
          fileTypesToAccept={this.validFileExtension}
          handleSubmit={this.handleUploadFile}
        />
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
      </>
    )
  }

  private handleUploadFile = (
    uploadContent: string,
    fileName: string
  ): void => {
    const {template, onUpdateTemplate} = this.props

    const fileExtensionRegex = new RegExp(`${this.validFileExtension}$`)
    if (!fileName.match(fileExtensionRegex)) {
      this.props.notify(notifyCSVUploadFailed())
      return
    }

    let templateValues

    if (uploadContent.trim() === '') {
      templateValues = []
    } else {
      templateValues = uploadContent.split(',').map(s => s.trim())
    }
    // account for newline separated values too.
    // should return values be strings only.

    this.setState({templateValues})

    const nextValues = templateValues.map((value: string): TemplateValue => {
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

  private get validFileExtension(): string {
    return '.csv'
  }
}

export default CSVFileTemplateBuilder
