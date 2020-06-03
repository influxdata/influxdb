import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {checkResultsLength} from 'src/shared/utils/vis'

// NOTE we dont want any pipe component to be directly dependent
// to any notebook concepts as this'll limit future reusability
// but timezone seems like an app setting, and its existance within
// the notebook folder is purely a convenience
import {AppSettingContext} from 'src/notebooks/context/app'

const Visualization: FC<PipeProp> = ({data, results, Context, loading}) => {
  const {timeZone} = useContext(AppSettingContext)

  return (
    <Context>
      <div className="notebook-visualization">
        <EmptyQueryView
          loading={loading}
          errorMessage={results.error}
          errorFormat={ErrorFormat.Scroll}
          hasResults={checkResultsLength(results.parsed)}
        >
          <ViewSwitcher
            giraffeResult={results.parsed}
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
