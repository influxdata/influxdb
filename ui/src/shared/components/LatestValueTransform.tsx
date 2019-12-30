// Libraries
import React, {useMemo, FunctionComponent} from 'react'
import {Table} from '@influxdata/giraffe'
import {isString} from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Utils
import {latestValues as getLatestValues} from 'src/shared/utils/latestValues'

interface Props {
  table: Table
  children: (latestValue: number) => JSX.Element
  allowString: boolean
  // If `quiet` is set and a latest value can't be found, this component will
  // display nothing instead of an empty graph error message
  quiet?: boolean
}

const LatestValueTransform: FunctionComponent<Props> = ({
  table,
  quiet = false,
  children,
  allowString,
}) => {
  const latestValues = useMemo(() => getLatestValues(table), [table])

  if (latestValues.length === 0 && quiet) {
    return null
  }

  if (latestValues.length === 0) {
    return <EmptyGraphMessage message="No latest value found" />
  }

  const latestValue = latestValues[0]

  if (isString(latestValue) && !allowString && quiet) {
    return null
  }

  if (isString(latestValue) && !allowString) {
    return (
      <EmptyGraphMessage message="String value cannot be displayed in this graph type" />
    )
  }

  return children(latestValue)
}

export default LatestValueTransform
