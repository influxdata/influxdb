import React, {SFC, ChangeEvent, KeyboardEvent} from 'react'

type OnFilterChangeHandler = (e: ChangeEvent<HTMLInputElement>) => void
type OnFilterKeyPress = (e: KeyboardEvent<HTMLInputElement>) => void

interface Props {
  searchTerm: string
  onFilterChange: OnFilterChangeHandler
  onFilterKeyPress: OnFilterKeyPress
}

const FuncSelectorInput: SFC<Props> = ({
  searchTerm,
  onFilterChange,
  onFilterKeyPress,
}) => (
  <input
    className="form-control input-sm flux-func--input"
    type="text"
    autoFocus={true}
    placeholder="Add a Function..."
    spellCheck={false}
    onChange={onFilterChange}
    onKeyDown={onFilterKeyPress}
    value={searchTerm}
  />
)

export default FuncSelectorInput
