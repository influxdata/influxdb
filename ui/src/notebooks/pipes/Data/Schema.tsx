// Libraries
import React, {FC, useContext, useCallback} from 'react'
import {Button} from '@influxdata/clockface'

// Components
import {SchemaContext} from 'src/notebooks/context/schemaProvider'
import {PipeContext} from 'src/notebooks/context/pipe'

const SchemaFetcher: FC = () => {
  const {data} = useContext(PipeContext)
  const {localFetchSchema} = useContext(SchemaContext)

  const selectedBucketName = data.bucketName

  const handleClick = useCallback(() => localFetchSchema(selectedBucketName), [
    localFetchSchema,
    selectedBucketName,
  ])

  return (
    <div className="fetch-schema--block">
      <Button
        className="fetch-schema--btn"
        text="Fetch Schema"
        onClick={handleClick}
      />
    </div>
  )
}

export default SchemaFetcher
