// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import EmptyTemplatesList from 'src/templates/components/EmptyTemplatesList'
import StaticTemplateCard from 'src/templates/components/StaticTemplateCard'

// Types
import {Template, TemplateSummary, RemoteDataState} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from 'src/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

export type TemplateOrSummary = Template | TemplateSummary

export interface StaticTemplate {
  name: string
  template: TemplateOrSummary
}

interface Props {
  templates: StaticTemplate[]
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  onImport: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
}

export default class StaticTemplatesList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {searchTerm, onImport} = this.props

    return (
      <ResourceList>
        <ResourceList.Body
          emptyState={
            <EmptyTemplatesList searchTerm={searchTerm} onImport={onImport} />
          }
        >
          {this.rows}
        </ResourceList.Body>
      </ResourceList>
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
        template={{...t.template, status: RemoteDataState.Done}}
        onFilterChange={onFilterChange}
      />
    ))
  }
}
