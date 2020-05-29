// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  EmptyState,
  IconFont,
  ComponentColor,
  Button,
} from '@influxdata/clockface'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
  onImport: () => void
}

const EmptyTemplatesList: FunctionComponent<Props> = ({
  searchTerm,
  onImport,
}) => {
  if (searchTerm === '') {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>
          Looks like you don't have any <b>Templates</b>, why not create one?
        </EmptyState.Text>
        <Button
          text="Import Template"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
          onClick={onImport}
        />
      </EmptyState>
    )
  }

  return (
    <EmptyState size={ComponentSize.Large}>
      <EmptyState.Text>No Templates match your search term</EmptyState.Text>
    </EmptyState>
  )
}

export default EmptyTemplatesList
