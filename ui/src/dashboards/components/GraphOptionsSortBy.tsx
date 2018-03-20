import React from 'react'
import Dropdown from 'src/shared/components/Dropdown'

interface TableColumn {
  internalName: string
  displayName: string
}

interface Props {
  sortByOptions: any[]
  onChooseSortBy: (any) => void
  selected: TableColumn
}

const GraphOptionsSortBy = ({sortByOptions, onChooseSortBy, selected} : Props) => {
  const selectedValue = selected.displayName || selected.internalName

  return <div className="form-group col-xs-6">
    <label>Sort By</label>
    <Dropdown
      items={sortByOptions}
      selected={selectedValue}
      buttonColor="btn-default"
      buttonSize="btn-sm"
      className="dropdown-stretch"
      onChoose={onChooseSortBy}
    />
  </div>
}

export default GraphOptionsSortBy
