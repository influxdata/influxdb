// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Grid,
  TextArea,
  Columns,
  InputType,
  ComponentSize,
} from '@influxdata/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {LineProtocolTab} from 'src/types'
import {WritePrecision} from '@influxdata/influx'

import {setLineProtocolBody} from 'src/dataLoaders/actions/dataLoaders'

interface Props {
  lineProtocolBody: string
  activeLPTab: LineProtocolTab
  precision: WritePrecision
  setLineProtocolBody: typeof setLineProtocolBody
  onURLChange: (url: string) => void
  urlInput: string
}

export default class extends PureComponent<Props> {
  public render() {
    const {lineProtocolBody, activeLPTab, urlInput} = this.props

    switch (activeLPTab) {
      case LineProtocolTab.UploadFile:
        return (
          <DragAndDrop
            submitText="Upload File"
            handleSubmit={this.handleSetLineProtocol}
            submitOnDrop={true}
            submitOnUpload={true}
            className="line-protocol--content"
          />
        )
      case LineProtocolTab.EnterManually:
        return (
          <TextArea
            value={lineProtocolBody}
            placeholder="Write text here"
            onChange={this.handleTextChange}
            testID="line-protocol--text-area"
            className="line-protocol--content"
          />
        )
      case LineProtocolTab.EnterURL:
        return (
          <Grid>
            <Grid.Row>
              <Grid.Column
                widthXS={Columns.Twelve}
                widthMD={Columns.Ten}
                offsetMD={Columns.One}
              >
                <Form.Element label="File URL:">
                  <Input
                    titleText="File URL:"
                    type={InputType.Text}
                    placeholder="http://..."
                    value={urlInput}
                    onChange={this.handleChange}
                    autoFocus={true}
                    size={ComponentSize.Large}
                  />
                </Form.Element>
              </Grid.Column>
            </Grid.Row>
          </Grid>
        )
    }
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target
    this.props.onURLChange(value)
  }

  private handleTextChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const {setLineProtocolBody} = this.props
    setLineProtocolBody(e.target.value)
  }

  private handleSetLineProtocol = (lpBody: string) => {
    const {setLineProtocolBody} = this.props
    setLineProtocolBody(lpBody)
  }
}
