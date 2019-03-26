// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import {EmptyState} from 'src/clockface'

interface Props {
  searchTerm: string
  onCreate: () => void
  totalCount: number
  onImportTask: () => void
}

export default class EmptyTasksLists extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate, totalCount, onImportTask} = this.props

    if (totalCount && searchTerm === '') {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={`All ${totalCount} of your Tasks are inactive`}
          />
        </EmptyState>
      )
    }

    if (searchTerm === '') {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={"Looks like you don't have any Tasks , why not create one?"}
            highlightWords={['Tasks']}
          />
          <AddResourceDropdown
            onSelectNew={onCreate}
            onSelectImport={onImportTask}
            resourceName="Task"
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="No Tasks match your search term" />
      </EmptyState>
    )
  }
}
