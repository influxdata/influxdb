import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'
import Papa from 'papaparse'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'

import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {
  notifyCSVUploadFailed,
  notifyInvalidMapType,
} from 'src/shared/copy/notifications'

import {TemplateBuilderProps, TemplateValueType} from 'src/types'
import {trimAndRemoveQuotes} from 'src/tempVars/utils'

interface State {
  templateValuesString: string
}

@ErrorHandling
class MapTemplateBuilder extends PureComponent<TemplateBuilderProps, State> {
  public constructor(props: TemplateBuilderProps) {
    super(props)
    const templateValues = props.template.values.map(v => v.value)
    const templateKeys = props.template.values.map(v => v.key)
    const templateValuesString = templateKeys
      .map((v, i) => `${v}, ${templateValues[i]}`)
      .join('\n')

    this.state = {
      templateValuesString,
    }
  }

  public render() {
    const {onUpdateDefaultTemplateValue, template} = this.props
    const {templateValuesString} = this.state

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
            Mapping contains <strong>{template.values.length}</strong> key-value
            pair{this.pluralizer}
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

  private get pluralizer(): string {
    return this.props.template.values.length === 1 ? '' : 's'
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

    const nextValues = this.constructValuesFromString(uploadContent)

    onUpdateTemplate({...template, values: nextValues})
  }

  private handleBlur = (): void => {
    const {template, onUpdateTemplate} = this.props
    const {templateValuesString} = this.state

    const values = this.constructValuesFromString(templateValuesString)

    onUpdateTemplate({...template, values})
  }

  private get validFileExtension(): string {
    return '.csv'
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    this.setState({templateValuesString: e.target.value})
  }

  private constructValuesFromString(templateValuesString: string) {
    const {notify} = this.props
    const trimmed = _.trimEnd(templateValuesString, '\n')
    const parsedTVS = Papa.parse(trimmed)
    const templateValuesData = getDeep<string[][]>(parsedTVS, 'data', [[]])

    if (templateValuesData.length === 0) {
      return
    }

    let arrayOfKeys = []
    let values = []
    _.forEach(templateValuesData, arr => {
      if (arr.length === 2 || (arr.length === 3 && arr[2] === '')) {
        const key = trimAndRemoveQuotes(arr[0])
        const value = trimAndRemoveQuotes(arr[1])

        if (!arrayOfKeys.includes(key) && key !== '') {
          values = [
            ...values,
            {
              type: TemplateValueType.Map,
              value,
              key,
              selected: false,
              localSelected: false,
            },
          ]
          arrayOfKeys = [...arrayOfKeys, key]
        }
      } else {
        notify(notifyInvalidMapType())
      }
    })
    return values
  }
}

export default MapTemplateBuilder
