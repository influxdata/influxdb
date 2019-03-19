// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'
import EmptyTemplatesList from 'src/templates/components/EmptyTemplatesList'
import TemplateCard from 'src/templates/components/TemplateCard'

// Types
import {TemplateSummary} from '@influxdata/influx'
import {Sort} from 'src/clockface'

interface Props {
  templates: TemplateSummary[]
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  onImport: () => void
}

type SortKey = 'meta.name'

interface State {
  sortKey: SortKey
  sortDirection: Sort
}

export default class OrgTemplatesList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: null,
      sortDirection: Sort.Descending,
    }
  }

  public render() {
    const {searchTerm, onImport} = this.props
    const {sortKey, sortDirection} = this.state

    const headerKeys: SortKey[] = ['meta.name']

    return (
      <>
        <ResourceList>
          <ResourceList.Header>
            <ResourceList.Sorter
              name="Name"
              sortKey={headerKeys[0]}
              sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <ResourceList.Sorter
              name="Type"
              sortKey={headerKeys[1]}
              sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body
            emptyState={
              <EmptyTemplatesList
                searchTerm={searchTerm}
                onCreate={onImport}
                onImport={onImport}
              />
            }
          >
            {this.sortedCards}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private cards = (templates: TemplateSummary[]): JSX.Element => {
    const {onFilterChange} = this.props
    const templateCards = (
      <>
        {templates.map(t => (
          <TemplateCard
            key={`template-id--${t.id}`}
            template={t}
            onFilterChange={onFilterChange}
          />
        ))}
      </>
    )
    return templateCards
  }

  private get sortedCards(): JSX.Element {
    const {templates} = this.props
    const {sortKey, sortDirection} = this.state

    if (templates.length) {
      return (
        <SortingHat<TemplateSummary>
          list={templates}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {this.cards}
        </SortingHat>
      )
    }

    return null
  }
}
