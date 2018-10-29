import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'
import {Button, ComponentColor, IconFont} from 'src/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'

interface Props {
  onCreateTask: () => void
  filterTasks: (searchTerm: string) => void
}

export default class TasksHeader extends PureComponent<Props> {
  public render() {
    const {onCreateTask, filterTasks} = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title="Tasks" />
        </Page.Header.Left>
        <Page.Header.Right>
          <SearchWidget
            placeholderText="Filter tasks by name..."
            onSearch={filterTasks}
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
}
