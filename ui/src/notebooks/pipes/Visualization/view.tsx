import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {ViewTypeDropdown} from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import {checkResultsLength} from 'src/shared/utils/vis'
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

  const updateType = (type: ViewType) => {
    const newView = createView(type)

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

  const controls = (
    <ViewTypeDropdown
      viewType={data.properties.type}
      onUpdateType={updateType}
    />
  )

  return (
    <Context controls={controls}>
      <div className="notebook-visualization">
        <div className="notebook-visualization--header" />
        <div className="notebook-visualization--view">
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
      </div>
    </Context>
  )
}

export default Visualization
