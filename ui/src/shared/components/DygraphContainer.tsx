// Libraries
import React, {SFC} from 'react'

// Components
import Dygraph from 'src/shared/components/dygraph/Dygraph'
import DygraphCell from 'src/shared/components/DygraphCell'
import DygraphTransformation from 'src/shared/components/DygraphTransformation'

// Types
import {Options} from 'src/external/dygraph'
import {DygraphViewProperties} from 'src/types/v2/dashboards'
import {FluxTable, RemoteDataState, TimeRange} from 'src/types'

interface Props {
  viewID: string
  tables: FluxTable[]
  loading: RemoteDataState
  properties: DygraphViewProperties
  dygraphOptions?: Partial<Options>
  timeRange?: TimeRange
  onZoom?: (range: TimeRange) => void
  children?: JSX.Element
}

const DygraphContainer: SFC<Props> = props => {
  const {
    tables,
    viewID,
    loading,
    children,
    properties,
    timeRange,
    onZoom,
    dygraphOptions,
  } = props

  const {axes, type, colors, queries} = properties

  return (
    <DygraphTransformation tables={tables}>
      {({labels, dygraphsData}) => (
        <DygraphCell loading={loading}>
          <Dygraph
            type={type}
            axes={axes}
            viewID={viewID}
            colors={colors}
            labels={labels}
            queries={queries}
            options={dygraphOptions}
            timeSeries={dygraphsData}
            timeRange={timeRange}
            onZoom={onZoom}
          >
            {children}
          </Dygraph>
        </DygraphCell>
      )}
    </DygraphTransformation>
  )
}

export default DygraphContainer
