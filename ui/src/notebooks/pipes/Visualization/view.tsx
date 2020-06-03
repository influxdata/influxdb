import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {ViewTypeDropdown} from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import {checkResultsLength} from 'src/shared/utils/vis'
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

    if (type === 'table' && results.parsed) {
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

    if ((type === 'histogram' || type === 'scatter') && results.parsed) {
      newView.properties.fillColumns = results.parsed.fluxGroupKeyUnion
    }

    if (type === 'scatter' && results.parsed) {
      newView.properties.symbolColumns = results.parsed.fluxGroupKeyUnion
    }

    if ((type === 'heatmap' || type === 'scatter') && results.parsed) {
      newView.properties.xColumn =
        ['_time', '_start', '_stop'].filter(field =>
          results.parsed.table.columnKeys.includes(field)
        )[0] || results.parsed.table.columnKeys[0]
      newView.properties.yColumn =
        ['_value'].filter(field =>
          results.parsed.table.columnKeys.includes(field)
        )[0] || results.parsed.table.columnKeys[0]

      console.log('neat', newView.properties)
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

  console.log('neat', data.properties)

  return (
    <Context controls={controls}>
      <div className="notebook-visualization">
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
