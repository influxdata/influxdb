// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {
  TechnoSpinner,
  ComponentSize,
  RemoteDataState,
  Dropdown,
  IconFont,
} from '@influxdata/clockface'

// Contexts
import {BucketContext} from 'src/notebooks/context/buckets'
import {PipeContext} from 'src/notebooks/context/pipe'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Types
import {Bucket} from 'src/types'

const BucketSelector: FC = () => {
  const {data, update} = useContext(PipeContext)
  const {buckets, loading} = useContext(BucketContext)
  const {localFetchSchema} = useContext(SchemaContext)
  const selectedBucketName = data?.bucketName
  let buttonText = 'Loading buckets...'

  const updateBucket = useCallback(
    (updatedBucket: Bucket): void => {
      event('Updating Bucket Selection in Flow Query Builder')
      localFetchSchema(updatedBucket.name)
      update({bucketName: updatedBucket.name})
    },
    [update, localFetchSchema]
  )

  let menuItems = (
    <Dropdown.ItemEmpty>
      <TechnoSpinner strokeWidth={ComponentSize.Small} diameterPixels={32} />
    </Dropdown.ItemEmpty>
  )

  if (loading === RemoteDataState.Done && buckets.length) {
    menuItems = (
      <>
        {buckets.map(bucket => (
          <Dropdown.Item
            key={bucket.name}
            value={bucket}
            onClick={updateBucket}
            selected={bucket.name === selectedBucketName}
            title={bucket.name}
            wrapText={true}
          >
            {bucket.name}
          </Dropdown.Item>
        ))}
      </>
    )
  }

  if (loading === RemoteDataState.Done && !selectedBucketName) {
    buttonText = 'Choose a bucket'
  } else if (loading === RemoteDataState.Done && selectedBucketName) {
    buttonText = selectedBucketName
  }

  const button = (active, onClick) => (
    <Dropdown.Button onClick={onClick} active={active} icon={IconFont.Disks}>
      {buttonText}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{menuItems}</Dropdown.Menu>
  )

  return <Dropdown button={button} menu={menu} style={{width: '250px'}} />
}

export default BucketSelector
