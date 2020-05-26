// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import EmptyTemplatesList from 'src/templates/components/EmptyTemplatesList'
import TemplateCard from 'src/templates/components/TemplateCard'

// Types
import {TemplateSummary} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from 'src/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

interface Props {
  templates: TemplateSummary[]
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  onImport: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
}

export default class TemplatesList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {searchTerm, onImport} = this.props

    return (
      <>
        <ResourceList>
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
      sortKey,
      sortDirection,
      sortType
    )

    return sortedTemplates.map(t => (
      <TemplateCard
        key={`template-id--${t.id}`}
        template={t}
        onFilterChange={onFilterChange}
      />
    ))
  }
}
