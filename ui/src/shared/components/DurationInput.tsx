// Libraries
import React, {useState, FC} from 'react'
import {
  Input,
  ComponentStatus,
  DropdownMenu,
  DropdownDivider,
  DropdownItem,
  ClickOutside,
} from '@influxdata/clockface'
import {isDurationParseable} from 'src/shared/utils/duration'

const SUGGESTION_CLASS = 'duration-input--suggestion'

type Props = {
  placeholder?: string
  exampleSearches: string[]
  onSubmit: (input: string) => void
  value: string
}

const DurationInput: FC<Props> = ({
  placeholder,
  exampleSearches,
  onSubmit,
  value,
}) => {
  const [isFocused, setIsFocused] = useState(false)

  const [input, setInput] = useState(value)

  const inputStatus = isDurationParseable(input)
    ? ComponentStatus.Default
    : ComponentStatus.Error

  const handleClickSuggestion = (suggestion: string) => {
    setInput(suggestion)

    onSubmit(suggestion)
    setIsFocused(false)
  }

  const handleClickOutside = e => {
    const didClickSuggestion =
      e.target.classList.contains(SUGGESTION_CLASS) ||
      e.target.parentNode.classList.contains(SUGGESTION_CLASS)

    if (!didClickSuggestion) {
      setIsFocused(false)
    }
  }

  const onChange = (i: string) => {
    setInput(i)
    if (isDurationParseable(i)) {
      onSubmit(i)
    }
  }

  return (
    <div className="status-search-bar">
      <ClickOutside onClickOutside={handleClickOutside}>
        <Input
          placeholder={placeholder}
          value={input}
          status={inputStatus}
          onChange={e => onChange(e.target.value)}
          onFocus={() => setIsFocused(true)}
        />
      </ClickOutside>
      {isFocused && (
        <DropdownMenu
          className="status-search-bar--suggestions"
          noScrollX={true}
          noScrollY={true}
        >
          <DropdownDivider text="Examples" />
          {exampleSearches.map(s => (
            <DropdownItem
              key={s}
              value={s}
              className={SUGGESTION_CLASS}
              onClick={handleClickSuggestion}
            >
              {s}
            </DropdownItem>
          ))}
        </DropdownMenu>
      )}
    </div>
  )
}

export default DurationInput
