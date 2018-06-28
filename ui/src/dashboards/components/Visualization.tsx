import React, {SFC} from 'react'
import {connect} from 'react-redux'

import RefreshingGraph from 'src/shared/components/RefreshingGraph'
import buildQueries from 'src/utils/buildQueriesForGraphs'
import VisualizationName from 'src/dashboards/components/VisualizationName'
import {SourceContext} from 'src/CheckSources'

import {getCellTypeColors} from 'src/dashboards/constants/cellEditor'

import {TimeRange, QueryConfig, Axes, Template, Source} from 'src/types'
import {
  TableOptions,
  DecimalPlaces,
  FieldOption,
  CellType,
} from 'src/types/dashboards'
import {ColorString, ColorNumber} from 'src/types/colors'

interface Props {
  type: CellType
  autoRefresh: number
  templates: Template[]
  timeRange: TimeRange
  queryConfigs: QueryConfig[]
  editQueryStatus: () => void
  axes: Axes
  tableOptions: TableOptions
  timeFormat: string
  decimalPlaces: DecimalPlaces
  fieldOptions: FieldOption[]
  resizerTopHeight: number
  thresholdsListColors: ColorNumber[]
  gaugeColors: ColorNumber[]
  lineColors: ColorString[]
  staticLegend: boolean
  isInCEO: boolean
}

const DashVisualization: SFC<Props> = ({
  axes,
  type,
  templates,
  timeRange,
  lineColors,
  autoRefresh,
  gaugeColors,
  queryConfigs,
  editQueryStatus,
  resizerTopHeight,
  staticLegend,
  thresholdsListColors,
  tableOptions,
  timeFormat,
  decimalPlaces,
  fieldOptions,
  isInCEO,
}) => {
  const colors: ColorString[] = getCellTypeColors({
    cellType: type,
    gaugeColors,
    thresholdsListColors,
    lineColors,
  })

  return (
    <div className="graph">
      <VisualizationName />
      <div className="graph-container">
        <SourceContext.Consumer>
          {(source: Source) => (
            <RefreshingGraph
              source={source}
              colors={colors}
              axes={axes}
              type={type}
              tableOptions={tableOptions}
              queries={buildQueries(queryConfigs, timeRange)}
              templates={templates}
              autoRefresh={autoRefresh}
              editQueryStatus={editQueryStatus}
              resizerTopHeight={resizerTopHeight}
              staticLegend={staticLegend}
              timeFormat={timeFormat}
              decimalPlaces={decimalPlaces}
              fieldOptions={fieldOptions}
              isInCEO={isInCEO}
            />
          )}
        </SourceContext.Consumer>
      </div>
    </div>
  )
}

const mapStateToProps = ({
  cellEditorOverlay: {
    thresholdsListColors,
    gaugeColors,
    lineColors,
    cell: {type, axes, tableOptions, fieldOptions, timeFormat, decimalPlaces},
  },
}) => ({
  gaugeColors,
  thresholdsListColors,
  lineColors,
  type,
  axes,
  tableOptions,
  fieldOptions,
  timeFormat,
  decimalPlaces,
})

export default connect(mapStateToProps, null)(DashVisualization)
