import React, {PureComponent, ChangeEvent} from 'react'

import {
  Input,
  InputType,
  Form,
  Grid,
  Columns,
  ComponentSize,
} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import TextArea from 'src/clockface/components/inputs/TextArea'
import {LineProtocolTab} from 'src/types/v2/dataLoaders'
import {setLineProtocolBody} from 'src/dataLoaders/actions/dataLoaders'
import {WritePrecision} from '@influxdata/influx'

interface Props {
  lineProtocolBody: string
  activeLPTab: LineProtocolTab
  precision: WritePrecision
  setLineProtocolBody: typeof setLineProtocolBody
  onURLChange: (url: string) => void
  urlInput: string
  handleSubmit?: () => void
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
          />
        )
      case LineProtocolTab.EnterManually:
        return (
          <TextArea
            value={lineProtocolBody}
            placeholder="Write text here"
            onChange={this.handleTextChange}
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

  private handleTextChange = async (lpBody: string) => {
    const {setLineProtocolBody} = this.props
    setLineProtocolBody(lpBody)
  }

  private handleSetLineProtocol = async (lpBody: string) => {
    const {setLineProtocolBody, handleSubmit} = this.props
    await setLineProtocolBody(lpBody)
    if (handleSubmit) {
      handleSubmit()
    }
  }
}
