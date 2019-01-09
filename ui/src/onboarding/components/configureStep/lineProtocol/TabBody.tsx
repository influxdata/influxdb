import React, {PureComponent, ChangeEvent} from 'react'

import {Input, InputType, Form} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import TextArea from 'src/clockface/components/inputs/TextArea'
import {LineProtocolTab} from 'src/types/v2/dataLoaders'
import {setLineProtocolBody} from 'src/onboarding/actions/dataLoaders'
import {WritePrecision} from 'src/api'

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
    const {
      setLineProtocolBody,
      lineProtocolBody,
      activeLPTab,
      urlInput,
    } = this.props

    switch (activeLPTab) {
      case LineProtocolTab.UploadFile:
        return (
          <DragAndDrop
            submitText="Upload File"
            handleSubmit={setLineProtocolBody}
            submitOnDrop={true}
            submitOnUpload={true}
          />
        )
      case LineProtocolTab.EnterManually:
        return (
          <TextArea
            value={lineProtocolBody}
            placeholder="Write text here"
            onChange={setLineProtocolBody}
          />
        )
      case LineProtocolTab.EnterURL:
        return (
          <Form.Element label="File URL:">
            <Input
              titleText="File URL:"
              type={InputType.Text}
              placeholder="http://..."
              widthPixels={700}
              value={urlInput}
              onChange={this.handleChange}
              autoFocus={true}
            />
          </Form.Element>
        )
    }
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target
    this.props.onURLChange(value)
  }
}
