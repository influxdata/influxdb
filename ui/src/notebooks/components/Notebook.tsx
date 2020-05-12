// Libraries
import React, {FunctionComponent, useState} from 'react'
import {connect} from 'react-redux'

// Components
import QueryBuilderPanel from 'src/notebooks/components/panels/QueryBuilderPanel'
import RawDataPanel from 'src/notebooks/components/panels/RawDataPanel'
import VisualizationPanel from 'src/notebooks/components/panels/VisualizationPanel'
import MarkdownPanel from 'src/notebooks/components/panels/MarkdownPanel'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  isViewingRawData: boolean
}

interface OwnProps {
  showMarkdownPanel: boolean
  onRemoveMarkdownPanel: () => void
}

type Props = OwnProps & StateProps

const Notebook: FunctionComponent<Props> = ({isViewingRawData, showMarkdownPanel, onRemoveMarkdownPanel}) => {
  const [queryName, setQueryName] = useState<string>('query_0')
  const [resultsName, setResultsName] = useState<string>('results_0')
  const [vizName, setVizName] = useState<string>('visualization_0')
  const [markdownText, updateMarkdownText] = useState<string>('')

  return (
    <div className="notebook">
      {showMarkdownPanel && <MarkdownPanel title="Markdown" contents={markdownText} onChangeContents={updateMarkdownText} onRemovePanel={onRemoveMarkdownPanel} />}
      <QueryBuilderPanel id={queryName} onChangeID={setQueryName} />
      <RawDataPanel
        id={resultsName}
        onChangeID={setResultsName}
        dataSourceName={queryName}
      />
      {isViewingRawData && <VisualizationPanel dataSourceName={resultsName} title={vizName} onChangeTitle={setVizName} />}
    </div>
  )
}

const mstp = (state: AppState) => {
  const {isViewingRawData} = getActiveTimeMachine(state)

  return {isViewingRawData}
}

export default connect<StateProps>(mstp)(Notebook)
