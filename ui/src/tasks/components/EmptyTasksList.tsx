// Libraries
import React, {PureComponent} from 'react'

// Components
import {EmptyState} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
  onCreate: () => void
  totalCount: number
  onImportTask: () => void
  onImportFromTemplate: () => void
}

export default class EmptyTasksLists extends PureComponent<Props> {
  public render() {
    const {
      searchTerm,
      onCreate,
      totalCount,
      onImportTask,
      onImportFromTemplate,
    } = this.props

    if (totalCount && searchTerm === '') {
      return (
        <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
          <EmptyState.Text>{`All ${totalCount} of your Tasks are inactive`}</EmptyState.Text>
        </EmptyState>
      )
    }

    if (searchTerm === '') {
      return (
        <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
          <EmptyState.Text>
            Looks like you don't have any <b>Tasks</b>, why not create one?"
          </EmptyState.Text>
          <AddResourceDropdown
            canImportFromTemplate
            onSelectNew={onCreate}
            onSelectImport={onImportTask}
            onSelectTemplate={onImportFromTemplate}
            resourceName="Task"
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
        <EmptyState.Text>No Tasks match your search term</EmptyState.Text>
      </EmptyState>
    )
  }
}
