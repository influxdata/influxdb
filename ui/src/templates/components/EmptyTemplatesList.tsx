// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  EmptyState,
  IconFont,
  ComponentColor,
  Button,
} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
}

const EmptyTemplatesList: FunctionComponent<Props> = ({searchTerm}) => {
  if (searchTerm === '') {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text
          text={"Looks like you don't have any Templates, why not create one?"}
          highlightWords={['Templates']}
        />
        <OverlayLink overlayID="import-template">
          {onClick => (
            <Button
              text="Import Template"
              icon={IconFont.Plus}
              color={ComponentColor.Primary}
              onClick={onClick}
            />
          )}
        </OverlayLink>
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
