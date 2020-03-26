// Libraries
import React, {FC, useState} from 'react'

// Components
import FluxFunctionsToolbar from 'src/timeMachine/components/fluxFunctionsToolbar/FluxFunctionsToolbar'
import VariableToolbar from 'src/timeMachine/components/variableToolbar/VariableToolbar'
import FluxToolbarTab from 'src/timeMachine/components/FluxToolbarTab'

// Types
import {FluxToolbarFunction} from 'src/types'

interface Props {
  activeQueryBuilderTab: string
  onInsertFluxFunction: (func: FluxToolbarFunction) => void
  onInsertVariable: (variableName: string) => void
}

type FluxToolbarTabs = 'functions' | 'variables'

const FluxToolbar: FC<Props> = ({
  activeQueryBuilderTab,
  onInsertFluxFunction,
  onInsertVariable,
}) => {
  const [activeTab, setActiveTab] = useState<FluxToolbarTabs>('functions')

  const handleTabClick = (id: FluxToolbarTabs): void => {
    setActiveTab(id)
  }

  let activeToolbar = (
    <FluxFunctionsToolbar onInsertFluxFunction={onInsertFluxFunction} />
  )

  if (activeTab === 'variables') {
    activeToolbar = <VariableToolbar onClickVariable={onInsertVariable} />
  }

  return (
    <div className="flux-toolbar">
      <div className="flux-toolbar--tabs">
        <FluxToolbarTab
          id="functions"
          onClick={handleTabClick}
          name="Functions"
          active={activeTab === 'functions'}
          testID="functions-toolbar-tab"
        />
        {activeQueryBuilderTab !== 'customCheckQuery' && (
          <FluxToolbarTab
            id="variables"
            onClick={handleTabClick}
            name="Variables"
            active={activeTab === 'variables'}
          />
        )}
      </div>
      <div className="flux-toolbar--tab-contents">{activeToolbar}</div>
    </div>
  )
}

export default FluxToolbar
