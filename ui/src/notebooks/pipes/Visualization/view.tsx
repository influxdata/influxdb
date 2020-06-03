import React, {FC} from 'react'
import {PipeProp} from 'src/notebooks'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {RemoteDataState} from 'src/types'
import {checkResultsLength} from 'src/shared/utils/vis'

const Visualization: FC<PipeProp> = ({data, results, Context}) => {
  const loading = results.parsed
    ? RemoteDataState.Done
    : RemoteDataState.NotStarted

  return (
    <Context>
      <div className="notebook-visualization">
        <EmptyQueryView
          loading={loading}
          errorFormat={ErrorFormat.Scroll}
          hasResults={checkResultsLength(results.parsed)}
        >
          <ViewSwitcher
            giraffeResult={results.parsed}
            loading={loading}
            theme="dark"
            properties={data.properties}
          />
        </EmptyQueryView>
      </div>
    </Context>
  )
}

export default Visualization
