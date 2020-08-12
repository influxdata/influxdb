// Libraries
import React, {PureComponent} from 'react'

// Components
import {EmptyState} from '@influxdata/clockface'
import AddResourceButton from 'src/shared/components/AddResourceButton'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
  onCreate: () => void
  totalCount: number
}

export default class EmptyTasksLists extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate, totalCount} = this.props

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
          <AddResourceButton onSelectNew={onCreate} resourceName="Task" />
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
