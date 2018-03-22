import React, {SFC} from 'react'

interface Props {
  filterText: string
  onEscape: (e: React.KeyboardEvent<HTMLInputElement>) => void
  onFilterText: (e: React.InputHTMLAttributes<HTMLInputElement>) => void
}

const MeasurementListFilter: SFC<Props> = ({
  onEscape,
  onFilterText,
  filterText,
}) => (
  <div className="query-builder--filter">
    <input
      className="form-control input-sm"
      placeholder="Filter"
      type="text"
      value={filterText}
      onChange={onFilterText}
      onKeyUp={onEscape}
      spellCheck={false}
      autoComplete="false"
    />
    <span className="icon search" />
  </div>
)

export default MeasurementListFilter
