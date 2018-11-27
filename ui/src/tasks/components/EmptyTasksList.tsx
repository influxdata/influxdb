// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Button,
  ComponentColor,
  ComponentSize,
  EmptyState,
  IconFont,
} from 'src/clockface'

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
            text={"Looks like you don't have any Tasks, why not create one?"}
          />
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

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text={'No tasks match your search term'} />
      </EmptyState>
    )
  }
}
