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
  EmptyState,
  IconFont,
} from 'src/clockface'

// Types
import {Task} from 'src/types/v2/tasks'
import {
  IndexListColumn,
  IndexListRow,
} from 'src/shared/components/index_views/IndexListTypes'

interface Props {
  tasks: Task[]
  onDelete: (task: Task) => void
  onCreate: () => void
}

export default class TasksList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList
        columns={this.columns}
        rows={this.rows}
        emptyState={this.emptyState}
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

  private get emptyState(): JSX.Element {
    const {onCreate} = this.props

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Looks like you don't have any Tasks, why not create one?" />
        <Button
          size={ComponentSize.Small}
          color={ComponentColor.Primary}
          icon={IconFont.Plus}
          text="Create Task"
          onClick={onCreate}
        />
      </EmptyState>
    )
  }
}
