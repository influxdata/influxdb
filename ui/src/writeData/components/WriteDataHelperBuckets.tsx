// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {WriteDataDetailsContext} from 'src/writeData/components/WriteDataDetailsContext'

// Components
import {
  List,
  ComponentSize,
  Heading,
  HeadingElement,
  Gradients,
  InfluxColors,
} from '@influxdata/clockface'

const WriteDataHelperBuckets: FC = () => {
  const {bucket, buckets, changeBucket} = useContext(WriteDataDetailsContext)

  return (
    <>
      <Heading
        element={HeadingElement.H6}
        className="write-data--details-widget-title"
      >
        Bucket
      </Heading>
      <List
        backgroundColor={InfluxColors.Obsidian}
        style={{height: '200px'}}
        maxHeight="200px"
      >
        {buckets.map(b => (
          <List.Item
            size={ComponentSize.Small}
            key={b.id}
            selected={b.id === bucket.id}
            value={b}
            onClick={changeBucket}
            wrapText={true}
            gradient={Gradients.GundamPilot}
          >
            {b.name}
          </List.Item>
        ))}
      </List>
    </>
  )
}

export default WriteDataHelperBuckets
