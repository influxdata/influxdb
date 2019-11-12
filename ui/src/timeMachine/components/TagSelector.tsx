// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  AlignItems,
  ComponentSize,
  FlexBox,
  FlexDirection,
  Input,
} from '@influxdata/clockface'
import SearchableDropdown from 'src/shared/components/SearchableDropdown'
import WaitingText from 'src/shared/components/WaitingText'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  selectTagKey,
  selectTagValue,
  searchTagValues,
  searchTagKeys,
  removeTagSelector,
  setKeysSearchTerm,
  setValuesSearchTerm,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'
import DefaultDebouncer from 'src/shared/utils/debouncer'
import {
  getActiveQuery,
  getActiveTimeMachine,
  getIsInCheckOverlay,
} from 'src/timeMachine/selectors'

// Types
import {AppState, RemoteDataState} from 'src/types'

const SEARCH_DEBOUNCE_MS = 500

interface StateProps {
  emptyText: string
  keys: string[]
  keysStatus: RemoteDataState
  selectedKey: string
  values: string[]
  valuesStatus: RemoteDataState
  selectedValues: string[]
  valuesSearchTerm: string
  keysSearchTerm: string
  isInCheckOverlay: boolean
}

interface DispatchProps {
  onSelectValue: typeof selectTagValue
  onSelectTag: typeof selectTagKey
  onSearchValues: typeof searchTagValues
  onSearchKeys: typeof searchTagKeys
  onRemoveTagSelector: typeof removeTagSelector
  onSetValuesSearchTerm: typeof setValuesSearchTerm
  onSetKeysSearchTerm: typeof setKeysSearchTerm
}

interface OwnProps {
  index: number
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
class TagSelector extends PureComponent<Props> {
  private debouncer = new DefaultDebouncer()

  public render() {
    const {index} = this.props

    return (
      <BuilderCard>
        <BuilderCard.Header
          title="Filter"
          onDelete={index !== 0 && this.handleRemoveTagSelector}
        />
        {this.body}
      </BuilderCard>
    )
  }

  private get body() {
    const {
      index,
      keys,
      keysStatus,
      selectedKey,
      emptyText,
      valuesSearchTerm,
      keysSearchTerm,
    } = this.props

    if (keysStatus === RemoteDataState.NotStarted) {
      return <BuilderCard.Empty>{emptyText}</BuilderCard.Empty>
    }

    if (keysStatus === RemoteDataState.Error) {
      return <BuilderCard.Empty>Failed to load tag keys</BuilderCard.Empty>
    }

    if (keysStatus === RemoteDataState.Done && !keys.length) {
      return (
        <BuilderCard.Empty testID="empty-tag-keys">
          No tag keys found <small>in the current time range</small>
        </BuilderCard.Empty>
      )
    }

    return (
      <>
        <BuilderCard.Menu testID={`tag-selector--container ${index}`}>
          <FlexBox
            direction={FlexDirection.Row}
            alignItems={AlignItems.Center}
            margin={ComponentSize.Small}
          >
            <SearchableDropdown
              searchTerm={keysSearchTerm}
              emptyText="No Tags Found"
              searchPlaceholder="Search keys..."
              selectedOption={selectedKey}
              onSelect={this.handleSelectTag}
              buttonStatus={toComponentStatus(keysStatus)}
              onChangeSearchTerm={this.handleKeysSearch}
              testID="tag-selector--dropdown"
              buttonTestID="tag-selector--dropdown-button"
              menuTestID="tag-selector--dropdown-menu"
              options={keys}
            />
            {this.selectedCounter}
          </FlexBox>
          <Input
            value={valuesSearchTerm}
            placeholder={`Search ${selectedKey} tag values`}
            className="tag-selector--search"
            onChange={this.handleValuesSearch}
          />
        </BuilderCard.Menu>
        {this.values}
      </>
    )
  }

  private get values() {
    const {selectedKey, values, valuesStatus, selectedValues} = this.props

    if (valuesStatus === RemoteDataState.Error) {
      return (
        <BuilderCard.Empty>
          {`Failed to load tag values for ${selectedKey}`}
        </BuilderCard.Empty>
      )
    }

    if (valuesStatus === RemoteDataState.Loading) {
      return (
        <BuilderCard.Empty>
          <WaitingText text="Loading tag values" />
        </BuilderCard.Empty>
      )
    }

    if (valuesStatus === RemoteDataState.Done && !values.length) {
      return (
        <BuilderCard.Empty>
          No values found <small>in the current time range</small>
        </BuilderCard.Empty>
      )
    }

    return (
      <SelectorList
        items={values}
        selectedItems={selectedValues}
        onSelectItem={this.handleSelectValue}
        multiSelect={!this.props.isInCheckOverlay}
      />
    )
  }

  private get selectedCounter(): JSX.Element {
    const {selectedValues} = this.props

    const pluralizer = selectedValues.length === 1 ? '' : 's'

    if (selectedValues.length > 0) {
      return (
        <div
          className="tag-selector--count"
          title={`${selectedValues.length} value${pluralizer} selected`}
        >
          {selectedValues.length}
        </div>
      )
    }
  }

  private handleSelectTag = (tag: string): void => {
    const {index, onSelectTag} = this.props

    onSelectTag(index, tag)
  }

  private handleSelectValue = (value: string): void => {
    const {index, onSelectValue} = this.props

    onSelectValue(index, value)
  }

  private handleRemoveTagSelector = (): void => {
    const {index, onRemoveTagSelector} = this.props

    onRemoveTagSelector(index)
  }

  private handleKeysSearch = (value: string) => {
    const {onSetKeysSearchTerm, index} = this.props

    onSetKeysSearchTerm(index, value)
    this.debouncer.call(this.emitKeysSearch, SEARCH_DEBOUNCE_MS)
  }

  private emitKeysSearch = () => {
    const {index, onSearchKeys} = this.props

    onSearchKeys(index)
  }

  private handleValuesSearch = (e: ChangeEvent<HTMLInputElement>) => {
    const {onSetValuesSearchTerm, index} = this.props
    const {value} = e.target

    onSetValuesSearchTerm(index, value)
    this.debouncer.call(this.emitValuesSearch, SEARCH_DEBOUNCE_MS)
  }

  private emitValuesSearch = () => {
    const {index, onSearchValues} = this.props

    onSearchValues(index)
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const {
    keys,
    keysStatus,
    values,
    valuesStatus,
    valuesSearchTerm,
    keysSearchTerm,
  } = getActiveTimeMachine(state).queryBuilder.tags[ownProps.index]

  const tags = getActiveQuery(state).builderConfig.tags
  const {key: selectedKey, values: selectedValues} = tags[ownProps.index]

  let emptyText: string

  if (ownProps.index === 0 || !tags[ownProps.index - 1].key) {
    emptyText = ''
  } else {
    emptyText = `Select a ${tags[ownProps.index - 1].key} value first`
  }

  const isInCheckOverlay = getIsInCheckOverlay(state)

  return {
    emptyText,
    keys,
    keysStatus,
    selectedKey,
    values,
    valuesStatus,
    selectedValues,
    valuesSearchTerm,
    keysSearchTerm,
    isInCheckOverlay,
  }
}

const mdtp = {
  onSelectValue: selectTagValue,
  onSelectTag: selectTagKey,
  onSearchValues: searchTagValues,
  onSearchKeys: searchTagKeys,
  onRemoveTagSelector: removeTagSelector,
  onSetKeysSearchTerm: setKeysSearchTerm,
  onSetValuesSearchTerm: setValuesSearchTerm,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TagSelector)
