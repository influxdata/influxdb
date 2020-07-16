// Libraries
import React, {useState, useEffect, FC} from 'react'
import {
  Input,
  DropdownMenu,
  DropdownDivider,
  DropdownItem,
  ClickOutside,
  ComponentStatus,
} from '@influxdata/clockface'
import {isDurationParseable} from 'src/shared/utils/duration'

const SUGGESTION_CLASS = 'duration-input--suggestion'

type Props = {
  suggestions: string[]
  onSubmit: (input: string) => void
  value: string
  placeholder?: string
  submitInvalid?: boolean
  showDivider?: boolean
  testID?: string
  validFunction?: (input: string) => boolean
  status?: ComponentStatus
}

const DurationInput: FC<Props> = ({
  suggestions,
  onSubmit,
  value,
  placeholder,
  status: controlledStatus,
  submitInvalid = true,
  showDivider = true,
  testID = 'duration-input',
  validFunction = _ => false,
}) => {
  const [isFocused, setIsFocused] = useState(false)

  const [inputValue, setInputValue] = useState(value)

  useEffect(() => {
    if (value != inputValue) {
      setInputValue(value)
    }
  }, [value, inputValue])

  const handleClickSuggestion = (suggestion: string) => {
    setInputValue(suggestion)

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

  const isValid = (i: string): boolean =>
    isDurationParseable(i) || validFunction(i)

  const getInputStatus = () => {
    if (controlledStatus === ComponentStatus.Default) {
      return isValid(inputValue)
        ? ComponentStatus.Default
        : ComponentStatus.Error
    }
    return controlledStatus || ComponentStatus.Default
  }

  const onChange = (i: string) => {
    setInputValue(i)
    if (submitInvalid || (!submitInvalid && isValid(i))) {
      onSubmit(i)
    }
  }

  return (
    <div className="status-search-bar">
      <ClickOutside onClickOutside={handleClickOutside}>
        <Input
          placeholder={placeholder}
          value={inputValue}
          status={getInputStatus()}
          onChange={e => onChange(e.target.value)}
          onFocus={() => setIsFocused(true)}
          testID={testID}
        />
      </ClickOutside>
      {isFocused && (
        <DropdownMenu
          className="status-search-bar--suggestions"
          noScrollX={true}
        >
          {showDivider && <DropdownDivider text="Examples" />}
          {suggestions.map(s => (
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
