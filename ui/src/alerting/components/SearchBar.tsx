// Libraries
import React, {useState, FC} from 'react'
import {
  Input,
  Icon,
  IconFont,
  ComponentStatus,
  DropdownMenu,
  DropdownDivider,
  DropdownItem,
  ClickOutside,
} from '@influxdata/clockface'

// Actions
import {search} from 'src/eventViewer/components/EventViewer.reducer'

// Utils
import {isSearchInputValid} from 'src/eventViewer/utils/search'

// Types
import {EventViewerChildProps} from 'src/eventViewer/types'

const SUGGESTION_CLASS = 'status-search-bar--suggestion'

type Props = EventViewerChildProps & {
  placeholder?: string
  exampleSearches: string[]
}

const SearchBar: FC<Props> = ({
  placeholder,
  exampleSearches,
  state,
  dispatch,
  loadRows,
}) => {
  const [isFocused, setIsFocused] = useState(false)

  const inputStatus = isSearchInputValid(state.searchInput)
    ? ComponentStatus.Default
    : ComponentStatus.Error

  const handleClickSuggestion = (suggestion: string) => {
    search(state, dispatch, loadRows, suggestion, true)
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

  return (
    <div className="status-search-bar">
      <ClickOutside onClickOutside={handleClickOutside}>
        <Input
          icon={IconFont.Search}
          placeholder={placeholder}
          style={{width: '100%'}}
          value={state.searchInput}
          status={inputStatus}
          onChange={e => search(state, dispatch, loadRows, e.target.value)}
          onFocus={() => setIsFocused(true)}
          testID="check-status-input"
        />
      </ClickOutside>
      {state.searchInput.trim() !== '' && (
        <div
          className="status-search-bar--clear"
          onClick={() => search(state, dispatch, loadRows, '', true)}
        >
          <Icon glyph={IconFont.Remove} />
        </div>
      )}
      {isFocused && (
        <DropdownMenu
          testID="check-status-dropdown"
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

export default SearchBar
