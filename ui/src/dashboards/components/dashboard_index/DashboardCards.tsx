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
  private _frame
  private _window
  private _observer
  private _spinner

  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  state = {
    hasMeasured: false,
    pages: 1,
    windowSize: 0,
  }

  public componentDidMount() {
    this.setState({
      hasMeasured: false,
      page: 1,
      windowSize: 0,
    })
  }

  public componentDidUpdate() {
    this.addMore()
  }

  private registerFrame = elem => {
    this._frame = elem
  }

  private registerWindow = elem => {
    this._window = elem
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
        pages: this.state.pages + 2,
      })
    }
  }

  private addMore = () => {
    if (this.state.hasMeasured) {
      return
    }

    if (
      this.state.windowSize * this.state.pages >=
      this.props.dashboards.length
    ) {
      return
    }

    if (!this._frame) {
      return
    }

    const frame = this._frame.getBoundingClientRect()
    const win = this._window.getBoundingClientRect()

    if (frame.height <= win.height) {
      this.setState(
        {
          windowSize: this.state.windowSize + 1,
        },
        () => {
          this.addMore()
        }
      )
    } else {
      this.setState({
        windowSize: this.state.windowSize,
        pages: 3,
        hasMeasured: true,
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

    const {windowSize, pages, hasMeasured} = this.state

    return (
      <div style={{height: '100%', display: 'grid'}} ref={this.registerFrame}>
        <div className="dashboards-card-grid" ref={this.registerWindow}>
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
        {hasMeasured && windowSize * pages < dashboards.length && (
          <div
            style={{height: '140px', margin: '24px auto'}}
            ref={this.registerSpinner}
          >
            <TechnoSpinner />
          </div>
        )}
      </div>
    )
  }
}
