// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'
// import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import EmptyTemplatesList from 'src/templates/components/EmptyTemplatesList'
import StaticTemplateCard from 'src/templates/components/StaticTemplateCard'

// Types
import {TemplateSummary} from '@influxdata/influx'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from 'src/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = 'meta.name'

interface Props {
  templates: {name: string; template: TemplateSummary}[]
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  onImport: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  title?: string
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

export default class StaticTemplatesList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {
      searchTerm,
      onImport,
      sortKey,
      sortDirection,
      onClickColumn,
    } = this.props

    const headerKeys: SortKey[] = ['meta.name']

    return (
      <>
        <h1>Static Templates</h1>
        <ResourceList>
          <ResourceList.Header>
            <ResourceList.Sorter
              name="Name"
              sortKey={headerKeys[0]}
              sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body
            emptyState={
              <EmptyTemplatesList searchTerm={searchTerm} onImport={onImport} />
            }
          >
            {this.rows}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get rows(): JSX.Element[] {
    const {
      templates,
      sortKey,
      sortDirection,
      sortType,
      onFilterChange,
    } = this.props

    const sortedTemplates = this.memGetSortedResources(
      templates,
      `template.${sortKey}`,
      sortDirection,
      sortType
    )

    return sortedTemplates.map(t => (
      <StaticTemplateCard
        key={`template-id--static-${t.name}`}
        name={t.name}
        template={t.template}
        onFilterChange={onFilterChange}
      />
    ))
  }
}
