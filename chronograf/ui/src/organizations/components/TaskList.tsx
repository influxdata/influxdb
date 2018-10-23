// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'

// Types
import {Task} from 'src/types/v2'

interface Props {
  tasks: Task[]
  emptyState: JSX.Element
}

export default class TaskList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="40%" />
          <IndexList.HeaderCell columnName="Owner" width="40%" />
          <IndexList.HeaderCell width="20%" />
        </IndexList.Header>
        <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    return this.props.tasks.map(task => (
      <IndexList.Row key={task.id}>
        <IndexList.Cell>
          <Link to={`/tasks/${task.id}`}>{task.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell>{task.owner.name}</IndexList.Cell>
        <IndexList.Cell revealOnHover={true}>DELETE</IndexList.Cell>
      </IndexList.Row>
    ))
  }
}
