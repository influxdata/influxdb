// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'
import {TechnoSpinner} from '@influxdata/clockface'

// Selectors
import {getSortedResources, SortTypes} from 'src/shared/utils/sort'

// Types
import {Dashboard, RemoteDataState} from 'src/types'
import {Sort} from 'src/clockface'

interface Props {
  dashboards: Dashboard[]
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onFilterChange: (searchTerm: string) => void
}

export default class DashboardCards extends PureComponent<Props> {
  private _observer
  private _spinner

  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  state = {
    pages: 1,
    windowSize: 0,
  }

  public componentDidMount() {
    this.setState({
      page: 1,
      windowSize: 15,
    })
  }

  private registerSpinner = elem => {
    this._spinner = elem

    if (!elem) return

    let count = 1.0
    const threshold = []

    while (count > 0) {
      threshold.push(count)
      count -= 0.1
    }

    threshold.reverse()

    this._observer = new IntersectionObserver(this.measure, {
      threshold,
      rootMargin: '60px 0px',
    })

    this._observer.observe(this._spinner)
  }

  private measure = entries => {
    if (
      entries
        .map(e => e.isIntersecting)
        .reduce((prev, curr) => prev || curr, false)
    ) {
      this.setState({
        pages: this.state.pages + 1,
      })
    }
  }

  public render() {
    const {
      dashboards,
      sortDirection,
      sortKey,
      sortType,
      onFilterChange,
    } = this.props
    const sortedDashboards = this.memGetSortedResources(
      dashboards,
      sortKey,
      sortDirection,
      sortType
    )

    const {windowSize, pages} = this.state

    return (
      <div style={{height: '100%', display: 'grid'}}>
        <div className="dashboards-card-grid">
          {sortedDashboards
            .filter(d => d.status === RemoteDataState.Done)
            .slice(0, pages * windowSize)
            .map(({id, name, description, labels, meta}) => (
              <DashboardCard
                key={id}
                id={id}
                name={name}
                labels={labels}
                updatedAt={meta.updatedAt}
                description={description}
                onFilterChange={onFilterChange}
              />
            ))}
        </div>
        {windowSize * pages < dashboards.length && (
          <div
            style={{height: '140px', margin: '14px auto'}}
            ref={this.registerSpinner}
          >
            <TechnoSpinner />
          </div>
        )}
      </div>
    )
  }
}
