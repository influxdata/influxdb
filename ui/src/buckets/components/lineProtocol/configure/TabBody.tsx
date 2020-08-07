// Libraries
import React, {FC, ChangeEvent, useContext} from 'react'

// Components
import {TextArea} from '@influxdata/clockface'
import DragAndDrop from 'src/buckets/components/lineProtocol/configure/DragAndDrop'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Action
import {setBody} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

interface Props {
  onSubmit: () => void
}

const TabBody: FC<Props> = ({onSubmit}) => {
  const [{body, tab}, dispatch] = useContext(Context)

  const handleTextChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    console.log(e.target.value)
    console.log('body: ', body)
    dispatch(setBody(e.target.value))
  }

  const handleSetBody = (b: string) => {
    dispatch(setBody(b))
  }

  switch (tab) {
    case 'Upload File':
      return (
        <DragAndDrop
          className="line-protocol--content"
          onSubmit={onSubmit}
          onSetBody={handleSetBody}
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
