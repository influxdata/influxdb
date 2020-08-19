// Libraries
import React, {FC, useEffect, useContext, useCallback} from 'react'

// Components
import {
  TechnoSpinner,
  ComponentSize,
  RemoteDataState,
  Dropdown,
} from '@influxdata/clockface'
import {BucketContext} from 'src/notebooks/context/buckets'
import {PipeContext} from 'src/notebooks/context/pipe'

// Types
import {Bucket} from 'src/types'

const BucketSelector: FC = () => {
  const {data, update} = useContext(PipeContext)
  const {buckets, loading} = useContext(BucketContext)
  const selectedBucketName = data?.bucketName

  const updateBucket = useCallback(
    (updatedBucket: Bucket): void => {
      update({bucketName: updatedBucket.name})
      // TODO(resetSchema based on updated selection)
      // resetSchema()
    },
    [update]
  )

  useEffect(() => {
    // selectedBucketName will only evaluate false on the initial render
    // because there is no default value
    if (!!buckets.length && !selectedBucketName) {
      updateBucket(buckets[0])
    }
  }, [buckets, selectedBucketName, updateBucket])

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

  const buttonText =
    loading === RemoteDataState.Done ? selectedBucketName : 'Loading...'

  const button = (active, onClick) => (
    <Dropdown.Button onClick={onClick} active={active}>
      {buttonText}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{menuItems}</Dropdown.Menu>
  )

  return <Dropdown button={button} menu={menu} style={{width: '250px'}} />
}

export default BucketSelector
