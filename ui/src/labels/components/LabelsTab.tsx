// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, EmptyState} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import CreateLabelOverlay from 'src/labels/components/CreateLabelOverlay'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import LabelList from 'src/labels/components/LabelList'
import FilterList from 'src/shared/components/FilterList'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'

// Actions
import {createLabel, updateLabel, deleteLabel} from 'src/labels/actions/thunks'

// Selectors
import {getAll} from 'src/resources/selectors'

// Utils
import {validateLabelUniqueness} from 'src/labels/utils/'

// Types
import {AppState, Label, ResourceType} from 'src/types'
import {
  IconFont,
  ComponentSize,
  ComponentColor,
  Sort,
} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {LabelSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  labels: Label[]
}

interface State {
  searchTerm: string
  isOverlayVisible: boolean
  sortKey: LabelSortKey
  sortDirection: Sort
  sortType: SortTypes
}

interface DispatchProps {
  createLabel: typeof createLabel
  updateLabel: typeof updateLabel
  deleteLabel: typeof deleteLabel
}

type Props = DispatchProps & StateProps

const FilterLabels = FilterList<Label>()
@ErrorHandling
class Labels extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      isOverlayVisible: false,
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render() {
    const {labels} = this.props
    const {
      searchTerm,
      isOverlayVisible,
      sortKey,
      sortDirection,
      sortType,
    } = this.state

    const leftHeaderItems = (
      <>
        <SearchWidget
          searchTerm={searchTerm}
          onSearch={this.handleFilterChange}
          placeholderText="Filter Labels..."
        />
        <ResourceSortDropdown
          resourceType={ResourceType.Labels}
          sortKey={sortKey}
          sortDirection={sortDirection}
          sortType={sortType}
          onSelect={this.handleSort}
        />
      </>
    )

    const rightHeaderItems = (
      <Button
        text="Create Label"
        color={ComponentColor.Primary}
        icon={IconFont.Plus}
        onClick={this.handleShowOverlay}
        testID="button-create"
      />
    )

    return (
      <>
        <TabbedPageHeader
          childrenLeft={leftHeaderItems}
          childrenRight={rightHeaderItems}
        />
        <FilterLabels
          list={labels}
          searchKeys={['name', 'properties.description']}
          searchTerm={searchTerm}
        >
          {ls => (
            <LabelList
              labels={ls}
              emptyState={this.emptyState}
              onUpdateLabel={this.handleUpdateLabel}
              onDeleteLabel={this.handleDelete}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
            />
          )}
        </FilterLabels>
        <CreateLabelOverlay
          isVisible={isOverlayVisible}
          onDismiss={this.handleDismissOverlay}
          onCreateLabel={this.handleCreateLabel}
          onNameValidation={this.handleNameValidation}
        />
      </>
    )
  }

  private handleSort = (
    sortKey: LabelSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private handleShowOverlay = (): void => {
    this.setState({isOverlayVisible: true})
  }

  private handleDismissOverlay = (): void => {
    this.setState({isOverlayVisible: false})
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private handleCreateLabel = (label: Label) => {
    this.props.createLabel(label.name, label.properties)
  }

  private handleUpdateLabel = (label: Label) => {
    this.props.updateLabel(label.id, label)
  }

  private handleDelete = (id: string) => {
    this.props.deleteLabel(id)
  }

  private handleNameValidation = (name: string): string | null => {
    const names = this.props.labels.map(label => label.name)

    return validateLabelUniqueness(names, name)
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text>No Labels match your search term</EmptyState.Text>
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text>
          Looks like you haven't created any <b>Labels</b>, why not create one?
        </EmptyState.Text>
        <Button
          text="Create Label"
          color={ComponentColor.Primary}
          icon={IconFont.Plus}
          onClick={this.handleShowOverlay}
          testID="button-create-initial"
        />
      </EmptyState>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const labels = getAll<Label>(state, ResourceType.Labels)
  return {labels}
}

const mdtp: DispatchProps = {
  createLabel: createLabel,
  updateLabel: updateLabel,
  deleteLabel: deleteLabel,
}

export default connect(
  mstp,
  mdtp
)(Labels)
