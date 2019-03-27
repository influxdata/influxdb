// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {EmptyState} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
  onCreate: () => void
  onImport: () => void
}

const EmptyTemplatesList: FunctionComponent<Props> = ({
  searchTerm,
  onCreate,
  onImport,
}) => {
  if (searchTerm === '') {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text
          text={"Looks like you don't have any Templates, why not create one?"}
          highlightWords={['Templates']}
        />
        <AddResourceDropdown
          onSelectNew={onCreate}
          onSelectImport={onImport}
          resourceName="Template"
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
