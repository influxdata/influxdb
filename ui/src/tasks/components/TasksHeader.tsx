// Libraries
import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'

// Components
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  SlideToggle,
} from 'src/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TaskOrgDropdown from 'src/tasks/components/TasksOrgDropdown'

interface Props {
  onCreateTask: () => void
  setSearchTerm: (searchTerm: string) => void
  setShowInactive: () => void
  showInactive: boolean
}

export default class TasksHeader extends PureComponent<Props> {
  public render() {
    const {
      onCreateTask,
      setSearchTerm,
      setShowInactive,
      showInactive,
    } = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title="Tasks" />
        </Page.Header.Left>
        <Page.Header.Right>
          <div>Show Inactive Tasks</div>
          <SlideToggle
            active={showInactive}
            size={ComponentSize.ExtraSmall}
            onChange={setShowInactive}
          />
          <SearchWidget
            placeholderText="Filter tasks by name..."
            onSearch={setSearchTerm}
          />
          <TaskOrgDropdown />
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
}
