// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import {Input, IconFont, ComponentSize, EmptyState} from 'src/clockface'
import TaskList from 'src/organizations/components/TaskList'
import FilterList from 'src/shared/components/Filter'

import {client} from 'src/utils/api'

// Types
import {Task} from 'src/api'

interface Props {
  tasks: Task[]
  orgName: string
  onChange: () => void
}

interface State {
  searchTerm: string
}

export default class Tasks extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    const {tasks} = this.props

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter tasks..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
        </TabbedPageHeader>
        <FilterList<Task>
          searchTerm={searchTerm}
          searchKeys={['name']}
          list={tasks}
        >
          {ts => (
            <TaskList
              tasks={ts}
              emptyState={this.emptyState}
              onDelete={this.handleDeleteTask}
              onUpdate={this.handleUpdateTask}
            />
          )}
        </FilterList>
      </>
    )
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Tasks , why not create one?`}
            highlightWords={'Tasks'}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Tasks match your query" />
      </EmptyState>
    )
  }

  private handleUpdateTask = async (task: Task) => {
    await client.tasks.update(task.id, task)
    this.props.onChange()
  }

  private handleDeleteTask = async (taskID: string) => {
    await client.tasks.delete(taskID)
    this.props.onChange()
  }
}
