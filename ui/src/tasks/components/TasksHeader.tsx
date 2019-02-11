// Libraries
import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'

// Components

import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
} from '@influxdata/clockface'
import {SlideToggle} from 'src/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TaskOrgDropdown from 'src/tasks/components/TasksOrgDropdown'

import 'src/tasks/components/TasksPage.scss'

interface Props {
  onCreateTask: () => void
  setSearchTerm: (searchTerm: string) => void
  setShowInactive: () => void
  showInactive: boolean
  toggleOverlay: () => void
  showOrgDropdown?: boolean
  showFilter?: boolean
}

export default class TasksHeader extends PureComponent<Props> {
  public static defaultProps: {
    showOrgDropdown: boolean
    showFilter: boolean
  } = {
    showOrgDropdown: true,
    showFilter: true,
  }

  public render() {
    const {
      onCreateTask,
      setShowInactive,
      showInactive,
      toggleOverlay,
    } = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title={this.pageTitle} />
        </Page.Header.Left>
        <Page.Header.Right>
          <SlideToggle.Label text="Show Inactive" />
          <SlideToggle
            active={showInactive}
            size={ComponentSize.ExtraSmall}
            onChange={setShowInactive}
          />
          {this.filterSearch}
          {this.orgDropDown}
          <Button
            text="Import"
            icon={IconFont.Import}
            onClick={toggleOverlay}
          />
          <Button
            color={ComponentColor.Primary}
            onClick={onCreateTask}
            icon={IconFont.Plus}
            text="Create Task"
            titleText="Create a new Task"
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }

  private get pageTitle() {
    const {showOrgDropdown} = this.props

    if (showOrgDropdown) {
      return 'Tasks'
    }
    return ''
  }

  private get filterSearch(): JSX.Element {
    const {setSearchTerm, showFilter} = this.props

    if (showFilter) {
      return (
        <SearchWidget
          placeholderText="Filter tasks by name..."
          onSearch={setSearchTerm}
        />
      )
    }
    return <></>
  }

  private get orgDropDown(): JSX.Element {
    const {showOrgDropdown} = this.props

    if (showOrgDropdown) {
      return <TaskOrgDropdown />
    }
    return <></>
  }
}
