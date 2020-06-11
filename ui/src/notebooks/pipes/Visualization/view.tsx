import React, {FC, useContext, useState} from 'react'
import {PipeProp} from 'src/notebooks'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import DashboardList from './DashboardList'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {ViewTypeDropdown} from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import {checkResultsLength} from 'src/shared/utils/vis'
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'
import {ViewType} from 'src/types'
import {createView} from 'src/views/helpers'

// NOTE we dont want any pipe component to be directly dependent
// to any notebook concepts as this'll limit future reusability
// but timezone seems like an app setting, and its existance within
// the notebook folder is purely a convenience
import {AppSettingContext} from 'src/notebooks/context/app'

const Visualization: FC<PipeProp> = ({
  data,
  results,
  onUpdate,
  Context,
  loading,
}) => {
  const {timeZone} = useContext(AppSettingContext)
  const [showExport, updateShowExport] = useState(false)

  const updateType = (type: ViewType) => {
    const newView = createView(type)

    // TODO: all of this needs to be removed by refactoring
    // the underlying logic. Managing state like this is a
    // recipe for long dev cycles, stale logic, and many bugs
    if (newView.properties.type === 'table' && results.parsed) {
      const existing = (newView.properties.fieldOptions || []).reduce(
        (prev, curr) => {
          prev[curr.internalName] = curr
          return prev
        },
        {}
      )

      results.parsed.table.columnKeys
        .filter(o => !existing.hasOwnProperty(o))
        .filter(o => !['result', '', 'table', 'time'].includes(o))
        .forEach(o => {
          existing[o] = {
            internalName: o,
            displayName: o,
            visible: true,
          }
        })
      const fieldOptions = Object.keys(existing).map(e => existing[e])
      newView.properties = {...newView.properties, fieldOptions}
    }

    if (
      (newView.properties.type === 'histogram' ||
        newView.properties.type === 'scatter') &&
      results.parsed
    ) {
      newView.properties.fillColumns = results.parsed.fluxGroupKeyUnion
    }

    if (newView.properties.type === 'scatter' && results.parsed) {
      newView.properties.symbolColumns = results.parsed.fluxGroupKeyUnion
    }

    if (
      (newView.properties.type === 'heatmap' ||
        newView.properties.type === 'scatter') &&
      results.parsed
    ) {
      newView.properties.xColumn =
        ['_time', '_start', '_stop'].filter(field =>
          results.parsed.table.columnKeys.includes(field)
        )[0] || results.parsed.table.columnKeys[0]
      newView.properties.yColumn =
        ['_value'].filter(field =>
          results.parsed.table.columnKeys.includes(field)
        )[0] || results.parsed.table.columnKeys[0]
    }

    onUpdate({
      properties: newView.properties,
    })
  }

  const exportStatus =
    !showExport && results.source
      ? ComponentStatus.Default
      : ComponentStatus.Disabled
  const toggleExport = () => {
    if (!results.source) {
      return
    }

    updateShowExport(!showExport)
  }

  const controls = (
    <>
      <ViewTypeDropdown
        viewType={data.properties.type}
        onUpdateType={updateType}
      />
      <SquareButton
        icon={IconFont.Export}
        onClick={toggleExport}
        titleText="Save to Dashboard"
        status={exportStatus}
      />
    </>
  )

  return (
    <Context controls={controls}>
      <div className="notebook-visualization">
        <DashboardList
          show={showExport}
          query={results.source}
          onClose={toggleExport}
          properties={data.properties}
        />
        <EmptyQueryView
          loading={loading}
          errorMessage={results.error}
          errorFormat={ErrorFormat.Scroll}
          hasResults={checkResultsLength(results.parsed)}
        >
          <ViewSwitcher
            giraffeResult={results.parsed}
            files={[results.raw]}
            loading={loading}
            properties={data.properties}
            timeZone={timeZone}
            theme="dark"
          />
        </EmptyQueryView>
      </div>
    </Context>
  )
}

export default Visualization
