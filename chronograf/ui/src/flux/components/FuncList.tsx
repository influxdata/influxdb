import React, {SFC, ChangeEvent, KeyboardEvent} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import FuncSelectorInput from 'src/shared/components/FuncSelectorInput'
import FuncListItem from 'src/flux/components/FuncListItem'

interface Props {
  inputText: string
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void
  onAddNode: (name: string) => void
  funcs: string[]
  selectedFunc: string
  onSetSelectedFunc: (name: string) => void
}

const FuncList: SFC<Props> = ({
  inputText,
  onAddNode,
  onKeyDown,
  onInputChange,
  funcs,
  selectedFunc,
  onSetSelectedFunc,
}) => {
  return (
    <div className="flux-func--autocomplete">
      <FuncSelectorInput
        onFilterChange={onInputChange}
        onFilterKeyPress={onKeyDown}
        searchTerm={inputText}
      />
      <ul className="flux-func--list">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={240}
          className="fancy-scroll--func-selector"
        >
          {!!funcs.length ? (
            funcs.map((func, i) => (
              <FuncListItem
                key={i}
                name={func}
                onAddNode={onAddNode}
                selectedFunc={selectedFunc}
                onSetSelectedFunc={onSetSelectedFunc}
              />
            ))
          ) : (
            <div className="flux-func--item empty">No matches</div>
          )}
        </FancyScrollbar>
      </ul>
    </div>
  )
}

export default FuncList
