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
  removeTagSelector,
  searchTagKeys,
  searchTagValues,
  selectTagKey,
  selectTagValue,
  setBuilderAggregateFunctionType,
  setKeysSearchTerm,
  setValuesSearchTerm,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import DefaultDebouncer from 'src/shared/utils/debouncer'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'
import {
  getActiveQuery,
  getActiveTagValues,
  getActiveTimeMachine,
  getIsInCheckOverlay,
} from 'src/timeMachine/selectors'

// Types
import {
  AppState,
  BuilderAggregateFunctionType,
  RemoteDataState,
} from 'src/types'

const SEARCH_DEBOUNCE_MS = 500

interface StateProps {
  aggregateFunctionType: BuilderAggregateFunctionType
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
  onRemoveTagSelector: typeof removeTagSelector
  onSearchKeys: typeof searchTagKeys
  onSearchValues: typeof searchTagValues
  onSelectTag: typeof selectTagKey
  onSelectValue: typeof selectTagValue
  onSetBuilderAggregateFunctionType: typeof setBuilderAggregateFunctionType
  onSetKeysSearchTerm: typeof setKeysSearchTerm
  onSetValuesSearchTerm: typeof setValuesSearchTerm
}

interface OwnProps {
  index: number
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
class TagSelector extends PureComponent<Props> {
  private debouncer = new DefaultDebouncer()

  private renderAggregateFunctionType(
    aggregateFunctionType: BuilderAggregateFunctionType
  ) {
    if (aggregateFunctionType === 'group') {
      return 'Group'
    }
    return 'Filter'
  }

  public render() {
    return (
      <BuilderCard>
        {this.header}
        {this.body}
      </BuilderCard>
    )
  }

  private get header() {
    const {aggregateFunctionType, index} = this.props

    return isFlagEnabled('queryBuilderGrouping') ? (
      <BuilderCard.DropdownHeader
        options={['filter', 'group']}
        selectedOption={this.renderAggregateFunctionType(aggregateFunctionType)}
        onDelete={index !== 0 && this.handleRemoveTagSelector}
        onSelect={this.handleAggregateFunctionSelect}
      />
    ) : (
      <BuilderCard.Header
        title={this.renderAggregateFunctionType(aggregateFunctionType)}
        onDelete={index !== 0 && this.handleRemoveTagSelector}
      />
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

  private handleAggregateFunctionSelect = (
    option: BuilderAggregateFunctionType
  ) => {
    const {index, onSetBuilderAggregateFunctionType} = this.props
    onSetBuilderAggregateFunctionType(option, index)
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const activeQueryBuilder = getActiveTimeMachine(state).queryBuilder

  const {
    keys,
    keysSearchTerm,
    keysStatus,
    valuesSearchTerm,
    valuesStatus,
  } = activeQueryBuilder.tags[ownProps.index]

  const tags = getActiveQuery(state).builderConfig.tags

  let emptyText: string
  const previousTagSelector = tags[ownProps.index - 1]
  if (
    ownProps.index === 0 ||
    !previousTagSelector ||
    !previousTagSelector.key
  ) {
    emptyText = ''
  } else {
    emptyText = `Select a ${tags[ownProps.index - 1].key} value first`
  }

  const {
    key: selectedKey,
    values: selectedValues,
    aggregateFunctionType,
  } = tags[ownProps.index]

  const values = getActiveTagValues(
    activeQueryBuilder.tags,
    aggregateFunctionType,
    ownProps.index
  )
  const isInCheckOverlay = getIsInCheckOverlay(state)

  return {
    aggregateFunctionType,
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
  onRemoveTagSelector: removeTagSelector,
  onSearchKeys: searchTagKeys,
  onSearchValues: searchTagValues,
  onSelectTag: selectTagKey,
  onSelectValue: selectTagValue,
  onSetBuilderAggregateFunctionType: setBuilderAggregateFunctionType,
  onSetKeysSearchTerm: setKeysSearchTerm,
  onSetValuesSearchTerm: setValuesSearchTerm,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TagSelector)
