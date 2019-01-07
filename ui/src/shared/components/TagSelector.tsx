// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Dropdown, Input, Button, ButtonShape, IconFont} from 'src/clockface'
import WaitingText from 'src/shared/components/WaitingText'
import SelectorList from 'src/shared/components/SelectorList'

// Actions
import {
  selectTagKey,
  selectTagValue,
  searchTagValues,
  removeTagSelector,
} from 'src/shared/actions/v2/queryBuilder'

// Utils
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'
import DefaultDebouncer from 'src/shared/utils/debouncer'
import {
  getActiveQuery,
  getActiveTimeMachine,
} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TagSelector.scss'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

const SEARCH_DEBOUNCE_MS = 500

interface StateProps {
  emptyText: string
  keys: string[]
  keysStatus: RemoteDataState
  selectedKey: string
  values: string[]
  valuesStatus: RemoteDataState
  selectedValues: string[]
}

interface DispatchProps {
  onSelectValue: (index: number, value: string) => void
  onSelectTag: (index: number, tag: string) => void
  onSearchValues: (index: number, searchTerm: string) => void
  onRemoveTagSelector: (index: number) => void
}

interface OwnProps {
  index: number
}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  searchTerm: string
}

class TagSelector extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  private debouncer = new DefaultDebouncer()

  public render() {
    return <div className="tag-selector">{this.body}</div>
  }

  private get body() {
    const {index, keys, keysStatus, selectedKey, emptyText} = this.props
    const {searchTerm} = this.state

    if (keysStatus === RemoteDataState.NotStarted) {
      return <div className="tag-selector--empty">{emptyText}</div>
    }

    if (keysStatus === RemoteDataState.Loading) {
      return (
        <div className="tag-selector--empty">
          <WaitingText text="Loading tag keys" />
        </div>
      )
    }

    if (keysStatus === RemoteDataState.Error) {
      return <div className="tag-selector--empty">Failed to load tag keys</div>
    }

    if (keysStatus === RemoteDataState.Done && !keys.length) {
      return <div className="tag-selector--empty">No more tag keys found</div>
    }

    return (
      <>
        <div className="tag-selector--top">
          <Dropdown
            selectedID={selectedKey}
            onChange={this.handleSelectTag}
            status={toComponentStatus(keysStatus)}
            titleText="No Tags Found"
          >
            {keys.map(key => (
              <Dropdown.Item key={key} id={key} value={key}>
                {key}
              </Dropdown.Item>
            ))}
          </Dropdown>
          {index !== 0 && (
            <Button
              shape={ButtonShape.Square}
              icon={IconFont.Remove}
              onClick={this.handleRemoveTagSelector}
              customClass="tag-selector--remove"
            />
          )}
        </div>
        <Input
          value={searchTerm}
          placeholder={`Search ${selectedKey} tag values`}
          customClass="tag-selector--search"
          onChange={this.handleSearch}
        />
        {this.values}
      </>
    )
  }

  private get values() {
    const {selectedKey, values, valuesStatus, selectedValues} = this.props

    if (valuesStatus === RemoteDataState.Error) {
      return (
        <div className="tag-selector--empty">
          {`Failed to load tag values for ${selectedKey}`}
        </div>
      )
    }

    if (valuesStatus === RemoteDataState.Loading) {
      return (
        <div className="tag-selector--empty">
          <WaitingText text="Loading tag values" />
        </div>
      )
    }

    if (valuesStatus === RemoteDataState.Done && !values.length) {
      return <div className="tag-selector--empty">Nothing found</div>
    }

    return (
      <SelectorList
        items={values}
        selectedItems={selectedValues}
        onSelectItem={this.handleSelectValue}
      />
    )
  }

  private handleSelectTag = (tag: string): void => {
    const {index, onSelectTag} = this.props

    onSelectTag(index, tag)
  }

  private handleSelectValue = (value: string): void => {
    const {index, onSelectValue} = this.props

    onSelectValue(index, value)
  }

  private handleRemoveTagSelector = () => {
    const {index, onRemoveTagSelector} = this.props

    onRemoveTagSelector(index)
  }

  private handleSearch = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target

    this.setState({searchTerm: value}, () => {
      this.debouncer.call(this.emitSearch, SEARCH_DEBOUNCE_MS)
    })
  }

  private emitSearch = () => {
    const {index, onSearchValues} = this.props
    const {searchTerm} = this.state

    onSearchValues(index, searchTerm)
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const {keys, keysStatus, values, valuesStatus} = getActiveTimeMachine(
    state
  ).queryBuilder.tags[ownProps.index]

  const tags = getActiveQuery(state).builderConfig.tags
  const {key: selectedKey, values: selectedValues} = tags[ownProps.index]

  let emptyText: string

  if (ownProps.index === 0 || !tags[ownProps.index - 1].key) {
    emptyText = ''
  } else {
    emptyText = `Select a ${tags[ownProps.index - 1].key} value first`
  }

  return {
    emptyText,
    keys,
    keysStatus,
    selectedKey,
    values,
    valuesStatus,
    selectedValues,
  }
}

const mdtp = {
  onSelectValue: selectTagValue,
  onSelectTag: selectTagKey,
  onSearchValues: searchTagValues,
  onRemoveTagSelector: removeTagSelector,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TagSelector)
