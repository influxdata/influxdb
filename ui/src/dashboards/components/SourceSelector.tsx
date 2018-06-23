import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'

import * as Types from 'src/types/modules'

interface Props {
  sources: Types.Sources.Data.SourceOption[]
  selected: string
  onSetQuerySource: (source: Types.Sources.Data.SourceOption) => void
  queries: Types.Queries.Data.QueryConfig[]
}

const SourceSelector: SFC<Props> = ({
  sources = [],
  selected,
  onSetQuerySource,
  queries,
}) =>
  sources.length > 1 && queries.length ? (
    <div className="source-selector">
      <h3>Source:</h3>
      <Dropdown
        items={sources}
        buttonSize="btn-sm"
        menuClass="dropdown-astronaut"
        useAutoComplete={true}
        selected={selected}
        onChoose={onSetQuerySource}
        className="dropdown-240"
      />
    </div>
  ) : (
    <div className="source-selector" />
  )

export default SourceSelector
