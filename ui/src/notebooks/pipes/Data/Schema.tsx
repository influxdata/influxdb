// Libraries
import React, {FC} from 'react'
import {Button} from '@influxdata/clockface'

type Props = {
  handleClick: () => void
}

const SchemaFetcher: FC<Props> = ({handleClick}) => {
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
