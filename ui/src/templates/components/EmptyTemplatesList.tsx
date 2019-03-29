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
        <EmptyState.Text
          text={"Looks like you don't have any Templates, why not create one?"}
          highlightWords={['Templates']}
        />
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
      <EmptyState.Text text="No Templates match your search term" />
    </EmptyState>
  )
}

export default EmptyTemplatesList
