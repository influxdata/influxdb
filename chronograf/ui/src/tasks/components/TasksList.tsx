// Libraries
import React, {PureComponent} from 'react'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'
import {
  ComponentSpacer,
  Alignment,
  Button,
  ComponentColor,
  ComponentSize,
} from 'src/clockface'

// Types
import {Task} from 'src/types/v2/tasks'
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'

interface Props {
  tasks: Task[]
  searchTerm: string
  onDelete: (task: Task) => void
  onCreate: () => void
}

export default class TasksList extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate} = this.props

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="65%" />
          <IndexList.HeaderCell columnName="Organization" width="15%" />
          <IndexList.HeaderCell columnName="Status" width="10%" />
          <IndexList.HeaderCell columnName="" width="10%" />
        </IndexList.Header>
        <IndexList.Body
          emptyState={
            <EmptyTasksList searchTerm={searchTerm} onCreate={onCreate} />
          }
          columnCount={4}
        >
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {tasks} = this.props

    return tasks.map(t => (
      <IndexList.Row key={`task-id--${t.id}`}>
        <IndexList.Cell>
          <a href="#">{t.name}</a>
        </IndexList.Cell>
        <IndexList.Cell>
          <a href="#">{t.organization.name}</a>
        </IndexList.Cell>
        <IndexList.Cell>Enabled</IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Danger}
              text="Delete"
              onClick={this.handleDeleteTask(t)}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    ))
  }

  private handleDeleteTask = (task: Task) => (): void => {
    const {onDelete} = this.props

    onDelete(task)
  }
}
