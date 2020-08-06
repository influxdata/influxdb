// Libraries
import React, {FC, ChangeEvent, useContext} from 'react'

// Components
import {TextArea} from '@influxdata/clockface'
import DragAndDrop from 'src/buckets/components/lineProtocol/configure/DragAndDrop'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Action
import {setBody} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

const TabBody: FC = () => {
  const [{body, tab}, dispatch] = useContext(Context)

  const handleTextChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    dispatch(setBody(e.target.value))
  }

  const handleSetLineProtocol = (body: string) => {
    dispatch(setBody(body))
  }

  switch (tab) {
    case 'Upload File':
      return (
        <DragAndDrop
          submitText="Upload File"
          handleSubmit={handleSetLineProtocol}
          submitOnDrop={true}
          submitOnUpload={true}
          className="line-protocol--content"
        />
      )
    case 'Enter Manually':
      return (
        <TextArea
          value={body}
          placeholder="Write text here"
          onChange={handleTextChange}
          testID="line-protocol--text-area"
          className="line-protocol--content"
        />
      )
  }
}

export default TabBody
