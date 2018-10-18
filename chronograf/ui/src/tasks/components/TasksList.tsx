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
import {
  IndexListColumn,
  IndexListRow,
} from 'src/shared/components/index_views/IndexListTypes'
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
      <IndexList
        columns={this.columns}
        rows={this.rows}
        emptyState={
          <EmptyTasksList searchTerm={searchTerm} onCreate={onCreate} />
        }
      />
    )
  }

  private get columns(): IndexListColumn[] {
    return [
      {
        key: 'task--name',
        title: 'Name',
        size: 500,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'task--organization',
        title: 'Organization',
        size: 100,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'task--status',
        title: 'Status',
        size: 90,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'task--actions',
        title: '',
        size: 200,
        showOnHover: true,
        align: Alignment.Right,
      },
    ]
  }

  private get rows(): IndexListRow[] {
    const {tasks} = this.props

    return tasks.map(t => ({
      disabled: false,
      columns: [
        {
          key: 'task--name',
          contents: <a href="#">{t.name}</a>,
        },
        {
          key: 'task--organization',
          contents: t.organization.name,
        },
        {
          key: 'task--status',
          contents: 'Enabled',
        },
        {
          key: 'task--actions',
          contents: (
            <ComponentSpacer align={Alignment.Right}>
              <Button
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Danger}
                text="Delete"
                onClick={this.handleDeleteTask(t)}
              />
            </ComponentSpacer>
          ),
        },
      ],
    }))
  }

  private handleDeleteTask = (task: Task) => (): void => {
    const {onDelete} = this.props

    onDelete(task)
  }
}
