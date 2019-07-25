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
          <EmptyState.Text
            text={`All ${totalCount} of your Tasks are inactive`}
          />
        </EmptyState>
      )
    }

    if (searchTerm === '') {
      return (
        <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
          <EmptyState.Text
            text={"Looks like you don't have any Tasks , why not create one?"}
            highlightWords={['Tasks']}
          />
          <AddResourceDropdown
            canImportFromTemplate={true}
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
        <EmptyState.Text text="No Tasks match your search term" />
      </EmptyState>
    )
  }
}
