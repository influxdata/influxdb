import React, {SFC, MouseEvent} from 'react'
import classnames from 'classnames'

import ConfirmOrCancel from 'src/shared/components/ConfirmOrCancel'
import SourceSelector from 'src/dashboards/components/SourceSelector'
import {QueryConfig, Source} from 'src/types'

interface SourceOption extends Source {
  text: string
}

interface Props {
  onCancel: () => void
  onSave: () => void
  isDisplayOptionsTabActive: boolean
  onClickDisplayOptions: (
    displayOptions: boolean
  ) => (event: MouseEvent<HTMLLIElement>) => void
  isSavable: boolean
  sources: SourceOption[]
  onSetQuerySource: (source: any) => void
  selected: string
  queries: QueryConfig[]
}

const OverlayControls: SFC<Props> = ({
  onSave,
  sources,
  queries,
  selected,
  onCancel,
  isSavable,
  onSetQuerySource,
  isDisplayOptionsTabActive,
  onClickDisplayOptions,
}) => (
  <div className="overlay-controls">
    <SourceSelector
      sources={sources}
      selected={selected}
      onSetQuerySource={onSetQuerySource}
      queries={queries}
    />
    <ul className="nav nav-tablist nav-tablist-sm">
      <li
        key="queries"
        className={classnames({
          active: !isDisplayOptionsTabActive,
        })}
        onClick={onClickDisplayOptions(false)}
      >
        Queries
      </li>
      <li
        key="displayOptions"
        className={classnames({
          active: isDisplayOptionsTabActive,
        })}
        onClick={onClickDisplayOptions(true)}
      >
        Visualization
      </li>
    </ul>
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
