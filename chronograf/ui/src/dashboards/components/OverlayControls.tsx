import React, {SFC} from 'react'

import ConfirmOrCancel from 'src/shared/components/ConfirmOrCancel'
import SourceSelector from 'src/dashboards/components/SourceSelector'
import RadioButtons from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {ButtonShape} from 'src/reusable_ui/types'

import {CEOTabs} from 'src/dashboards/constants'

import * as QueriesModels from 'src/types/queries'
import * as SourcesModels from 'src/types/sources'

interface Props {
  onCancel: () => void
  onSave: () => void
  activeEditorTab: CEOTabs
  onSetActiveEditorTab: (tabName: CEOTabs) => void
  isSavable: boolean
  sources: SourcesModels.SourceOption[]
  onSetQuerySource: (source: SourcesModels.Source) => void
  selected: string
  queries: QueriesModels.QueryConfig[]
}

const OverlayControls: SFC<Props> = ({
  onSave,
  sources,
  queries,
  selected,
  onCancel,
  isSavable,
  onSetQuerySource,
  activeEditorTab,
  onSetActiveEditorTab,
}) => (
  <div className="overlay-controls">
    <SourceSelector
      sources={sources}
      selected={selected}
      onSetQuerySource={onSetQuerySource}
      queries={queries}
    />
    <div className="overlay-controls--tabs">
      <RadioButtons
        activeButton={activeEditorTab}
        buttons={[CEOTabs.Queries, CEOTabs.Vis]}
        onChange={onSetActiveEditorTab}
        shape={ButtonShape.StretchToFit}
      />
    </div>
    <div className="overlay-controls--right">
      <ConfirmOrCancel
        onCancel={onCancel}
        onConfirm={onSave}
        isDisabled={!isSavable}
      />
    </div>
  </div>
)

export default OverlayControls
