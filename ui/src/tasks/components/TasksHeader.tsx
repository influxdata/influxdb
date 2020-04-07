// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  InputLabel,
  SlideToggle,
  ComponentSize,
  ComponentStatus,
  Page,
  Sort,
  FlexBox,
  FlexDirection,
} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TasksSortDropdown from 'src/tasks/components/TasksSortDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'
import {setSearchTerm as setSearchTermAction} from 'src/tasks/actions/creators'
import {SortKey} from 'src/tasks/containers/TasksPage'
import {SortTypes} from 'src/shared/utils/sort'

interface Props {
  onCreateTask: () => void
  setShowInactive: () => void
  showInactive: boolean
  onImportTask: () => void
  limitStatus: LimitStatus
  onImportFromTemplate: () => void
  searchTerm: string
  setSearchTerm: typeof setSearchTermAction
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
  onSort: (sortKey: SortKey, sortDirection: Sort, sortType: SortTypes) => void
}

export default class TasksHeader extends PureComponent<Props> {
  public render() {
    const {
      onCreateTask,
      setShowInactive,
      showInactive,
      onImportTask,
      onImportFromTemplate,
      setSearchTerm,
      searchTerm,
      sortKey,
      sortType,
      sortDirection,
      onSort,
    } = this.props

    return (
      <>
        <Page.Header fullWidth={false}>
          <Page.Title title="Tasks" />
          <CloudUpgradeButton />
        </Page.Header>
        <Page.ControlBar fullWidth={false}>
          <Page.ControlBarLeft>
            <SearchWidget
              placeholderText="Filter tasks..."
              onSearch={setSearchTerm}
              searchTerm={searchTerm}
            />
            <TasksSortDropdown
              sortKey={sortKey}
              sortType={sortType}
              sortDirection={sortDirection}
              onSelect={onSort}
            />
          </Page.ControlBarLeft>
          <Page.ControlBarRight>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Medium}
            >
              <InputLabel>Show Inactive</InputLabel>
              <SlideToggle
                active={showInactive}
                size={ComponentSize.ExtraSmall}
                onChange={setShowInactive}
              />
            </FlexBox>
            <AddResourceDropdown
              canImportFromTemplate
              onSelectNew={onCreateTask}
              onSelectImport={onImportTask}
              onSelectTemplate={onImportFromTemplate}
              resourceName="Task"
              status={this.addResourceStatus}
            />
          </Page.ControlBarRight>
        </Page.ControlBar>
      </>
    )
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}
