// Libraries
import React, {PureComponent} from 'react'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CollectorList from 'src/organizations/components/CollectorList'

import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  EmptyState,
} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Telegraf} from 'src/api'

interface Props {
  collectors: Telegraf[]
}

@ErrorHandling
export default class OrgOptions extends PureComponent<Props> {
  public render() {
    const {collectors} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Collectors</h1>
          <Button
            text="Create Collector"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
          />
        </TabbedPageHeader>
        <CollectorList collectors={collectors} emptyState={this.emptyState} />
      </>
    )
  }
  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Collectors match your query" />
      </EmptyState>
    )
  }
}
