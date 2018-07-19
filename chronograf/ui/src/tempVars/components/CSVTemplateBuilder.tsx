import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'
import Papa from 'papaparse'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'

import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {notifyCSVUploadFailed} from 'src/shared/copy/notifications'
import {trimAndRemoveQuotes} from 'src/tempVars/utils'

import {TemplateBuilderProps, TemplateValueType, TemplateValue} from 'src/types'

interface State {
  templateValuesString: string
}

@ErrorHandling
class CSVTemplateBuilder extends PureComponent<TemplateBuilderProps, State> {
  public constructor(props: TemplateBuilderProps) {
    super(props)
    const templateValues = props.template.values.map(v => v.value)

    this.state = {
      templateValuesString: templateValues.join(', '),
    }
  }

  public render() {
    const {onUpdateDefaultTemplateValue, template} = this.props
    const {templateValuesString} = this.state
    const pluralizer = template.values.length === 1 ? '' : 's'

    return (
      <>
        <div className="form-group col-xs-12">
          <label>Upload a CSV File</label>
          <DragAndDrop
            submitText="Preview"
            fileTypesToAccept={this.validFileExtension}
            handleSubmit={this.handleUploadFile}
            submitOnDrop={true}
            submitOnUpload={true}
            compact={true}
          />
        </div>
        <div className="form-group col-xs-12">
          <label>Comma Separated Values</label>
          <div className="temp-builder--mq-controls">
            <textarea
              className="form-control input-sm"
              value={templateValuesString}
              onChange={this.handleChange}
              onBlur={this.handleBlur}
            />
          </div>
        </div>
        <div className="form-group col-xs-12 temp-builder--results">
          <p className="temp-builder--validation">
            CSV contains <strong>{template.values.length}</strong> value{
              pluralizer
            }
          </p>
          {template.values.length > 0 && (
            <TemplatePreviewList
              items={template.values}
              onUpdateDefaultTemplateValue={onUpdateDefaultTemplateValue}
            />
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

    this.setState({templateValuesString: uploadContent})

    const nextValues = this.getValuesFromString(uploadContent)

    onUpdateTemplate({...template, values: nextValues})
  }

  private handleBlur = (): void => {
    const {template, onUpdateTemplate} = this.props
    const {templateValuesString} = this.state

    const nextValues = this.getValuesFromString(templateValuesString)

    onUpdateTemplate({...template, values: nextValues})
  }

  private get validFileExtension(): string {
    return '.csv'
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    this.setState({templateValuesString: e.target.value})
  }

  private getValuesFromString(templateValuesString) {
    const parsedTVS = Papa.parse(templateValuesString)
    const templateValuesData = getDeep<string[][]>(parsedTVS, 'data', [[]])

    const templateValues = _.filter(
      _.map(_.uniq(_.flatten(templateValuesData)), elt =>
        trimAndRemoveQuotes(elt)
      ),
      elt => elt !== ''
    )

    // check for too many errors in papa parse response
    const nextValues = templateValues.map((value: string): TemplateValue => {
      return {
        type: TemplateValueType.CSV,
        value,
        selected: false,
        localSelected: false,
      }
    })

    return nextValues
  }
}

export default CSVTemplateBuilder
