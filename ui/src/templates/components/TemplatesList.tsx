// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import EmptyTemplatesList from 'src/templates/components/EmptyTemplatesList'
import TemplateCard from 'src/templates/components/TemplateCard'

// Types
import {TemplateSummary} from '@influxdata/influx'
import {SortTypes} from 'src/shared/selectors/sort'
import {AppState} from 'src/types'
import {Sort} from 'src/clockface'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

type SortKey = 'meta.name'

interface OwnProps {
  templates: TemplateSummary[]
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  onImport: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedTemplates: TemplateSummary[]
}

type Props = OwnProps & StateProps

class TemplatesList extends PureComponent<Props> {
  public state = {
    sortedTemplates: this.props.sortedTemplates,
  }

  componentDidUpdate(prevProps) {
    const {templates, sortedTemplates, sortKey, sortDirection} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.templates.length !== templates.length
    ) {
      this.setState({sortedTemplates})
    }
  }

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
    const {onFilterChange} = this.props
    const {sortedTemplates} = this.state

    return sortedTemplates.map(t => (
      <TemplateCard
        key={`template-id--${t.id}`}
        template={t}
        onFilterChange={onFilterChange}
      />
    ))
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedTemplates: getSortedResource(state.templates.items, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(TemplatesList)
