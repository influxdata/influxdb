// Libraries
import React, {useMemo, FunctionComponent} from 'react'
import {Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Utils
import {latestValues as getLatestValues} from 'src/shared/utils/latestValues'

interface Props {
  table: Table
  children: (latestValue: number) => JSX.Element

  // If `quiet` is set and a latest value can't be found, this component will
  // display nothing instead of an empty graph error message
  quiet?: boolean
}

const LatestValueTransform: FunctionComponent<Props> = ({
  table,
  quiet = false,
  children,
}) => {
  const latestValues = useMemo(() => getLatestValues(table), [table])

  if (latestValues.length === 0 && quiet) {
    return null
  }

  console.log(latestValues)
  if (latestValues.length === 0) {
    return <EmptyGraphMessage message="No latest value found" />
  }

  const latestValue = latestValues[0]

  return children(latestValue)
}

export default LatestValueTransform
