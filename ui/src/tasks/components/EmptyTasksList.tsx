// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize} from '@influxdata/clockface'
import {EmptyState} from 'src/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

interface Props {
  searchTerm: string
  onCreate: () => void
  totalCount: number
  onImport: () => void
}

export default class EmptyTasksLists extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate, totalCount, onImport} = this.props

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
            onSelectImport={onImport}
            resourceName="Task"
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text={'No Tasks match your search term'} />
      </EmptyState>
    )
  }
}
