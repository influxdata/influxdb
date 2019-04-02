// Libraries
import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'

// Components
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import {Tabs, ComponentSpacer, Alignment, Stack} from 'src/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

interface Props {
  onCreateTask: () => void
  setShowInactive: () => void
  showInactive: boolean
  onImportTask: () => void
  showOrgDropdown?: boolean
  isFullPage?: boolean
  filterComponent: () => JSX.Element
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
      onImportTask,
      isFullPage,
      filterComponent,
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
            <AddResourceDropdown
              onSelectNew={onCreateTask}
              onSelectImport={onImportTask}
              resourceName="Task"
            />
          </Page.Header.Right>
        </Page.Header>
      )
    }

    return (
      <Tabs.TabContentsHeader>
        {filterComponent()}
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
            onSelectImport={onImportTask}
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
}
