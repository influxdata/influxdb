import React, {SFC, ChangeEvent, KeyboardEvent} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import DropdownInput from 'src/shared/components/DropdownInput'
import FuncListItem from 'src/ifql/components/FuncListItem'

interface Props {
  inputText: string
  isOpen: boolean
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void
  onAddNode: (name: string) => void
  funcs: string[]
}

const FuncList: SFC<Props> = ({
  inputText,
  isOpen,
  onAddNode,
  onKeyDown,
  onInputChange,
  funcs,
}) => {
  return (
    <ul className="dropdown-menu funcs">
      <DropdownInput
        buttonSize="btn-xs"
        buttonColor="btn-default"
        onFilterChange={onInputChange}
        onFilterKeyPress={onKeyDown}
        searchTerm={inputText}
      />
      <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={240}>
        {isOpen &&
          funcs.map((func, i) => (
            <FuncListItem key={i} name={func} onAddNode={onAddNode} />
          ))}
      </FancyScrollbar>
    </ul>
  )
}

export default FuncList
