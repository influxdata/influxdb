import React from 'react'
import Dropdown from 'src/shared/components/Dropdown'

interface Props {
  sortByOptions: any[]
  onChooseSortBy: (any) => void
  selected: string
}

const GraphOptionsSortBy = ({sortByOptions, onChooseSortBy, selected} : Props) => (
  <div className="form-group col-xs-6">
    <label>Sort By</label>
    <Dropdown
      items={sortByOptions}
      selected={selected}
      buttonColor="btn-default"
      buttonSize="btn-sm"
      className="dropdown-stretch"
      onChoose={onChooseSortBy}
    />
  </div>
)

export default GraphOptionsSortBy
