// Libraries
import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'

// Components
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import {Tabs, ComponentSpacer, Alignment, Stack} from 'src/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TaskOrgDropdown from 'src/tasks/components/TasksOrgDropdown'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

import 'src/tasks/components/TasksPage.scss'

interface Props {
  onCreateTask: () => void
  setSearchTerm: (searchTerm: string) => void
  setShowInactive: () => void
  showInactive: boolean
  toggleOverlay: () => void
  showOrgDropdown?: boolean
  isFullPage?: boolean
  searchTerm: string
}

export default class TasksHeader extends PureComponent<Props> {
  public static defaultProps: {
    showOrgDropdown: boolean
    isFullPage: boolean
  } = {
    showOrgDropdown: true,
    isFullPage: true,
  }

  public render() {
    const {
      onCreateTask,
      setShowInactive,
      showInactive,
      toggleOverlay,
      isFullPage,
    } = this.props

    if (isFullPage) {
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
            <AddResourceDropdown
              onSelectNew={onCreateTask}
              onSelectImport={toggleOverlay}
              resourceName="Task"
            />
          </Page.Header.Right>
        </Page.Header>
      )
    }

    return (
      <Tabs.TabContentsHeader>
        {this.filterSearch}
        <ComponentSpacer align={Alignment.Right} stackChildren={Stack.Columns}>
          <SlideToggle.Label text="Show Inactive" />
          <SlideToggle
            active={showInactive}
            size={ComponentSize.ExtraSmall}
            onChange={setShowInactive}
            testID="tasks-header--toggle-active"
          />
          <AddResourceDropdown
            onSelectNew={onCreateTask}
            onSelectImport={toggleOverlay}
            resourceName="Task"
          />
        </ComponentSpacer>
      </Tabs.TabContentsHeader>
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
    const {setSearchTerm, searchTerm} = this.props

    return (
      <SearchWidget
        placeholderText="Filter tasks by name..."
        onSearch={setSearchTerm}
        searchTerm={searchTerm}
      />
    )
  }

  private get orgDropDown(): JSX.Element {
    const {showOrgDropdown} = this.props

    if (showOrgDropdown) {
      return <TaskOrgDropdown />
    }
    return <></>
  }
}
