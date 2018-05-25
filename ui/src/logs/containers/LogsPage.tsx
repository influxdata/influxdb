import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {getSourceAsync, setTimeRange, setNamespace} from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import {Source, Namespace, TimeRange} from 'src/types'
import LogViewerHeader from 'src/logs/components/LogViewerHeader'
import Graph from 'src/logs/components/LogsGraph'
import Table from 'src/logs/components/LogsTable'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/LogsFilterBar'

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
  enabled: boolean
}

interface Props {
  sources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace
  getSource: (sourceID: string) => void
  getSources: () => void
  setTimeRange: (timeRange: TimeRange) => void
  setNamespace: (namespace: Namespace) => void
  timeRange: TimeRange
}

interface State {
  searchString: string
  filters: Filter[]
}

class LogsPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchString: '',
      filters: [],
    }
  }

  public componentDidUpdate() {
    if (!this.props.currentSource) {
      this.props.getSource(this.props.sources[0].id)
    }
  }

  public componentDidMount() {
    this.props.getSources()
  }

  public render() {
    const {searchString, filters} = this.state

    return (
      <div className="page">
        {this.header}
        <div className="page-contents logs-viewer">
          <Graph thing="wooo" />
          <SearchBar
            searchString={searchString}
            onChange={this.handleSearchInputChange}
            onSearch={this.handleSubmitSearch}
          />
          <FilterBar
            numResults={300}
            filters={filters}
            onUpdateFilters={this.handleUpdateFilters}
          />
          <Table thing="snooo" />
        </div>
      </div>
    )
  }

  private get header(): JSX.Element {
    const {
      sources,
      currentSource,
      currentNamespaces,
      timeRange,
      currentNamespace,
    } = this.props

    return (
      <LogViewerHeader
        availableSources={sources}
        timeRange={timeRange}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        onChooseTimerange={this.handleChooseTimerange}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
      />
    )
  }

  private handleSearchInputChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    this.setState({searchString: e.target.value})
  }

  private handleSubmitSearch = (): void => {
    // do the thing
  }

  private handleUpdateFilters = (filters: Filter[]): void => {
    this.setState({filters})
  }

  private handleChooseTimerange = (timeRange: TimeRange) => {
    this.props.setTimeRange(timeRange)
  }

  private handleChooseSource = (sourceID: string) => {
    this.props.getSource(sourceID)
  }

  private handleChooseNamespace = (namespace: Namespace) => {
    // Do flip
    this.props.setNamespace(namespace)
  }
}

const mapStateToProps = ({
  sources,
  logs: {currentSource, currentNamespaces, timeRange, currentNamespace},
}) => ({
  sources,
  currentSource,
  currentNamespaces,
  timeRange,
  currentNamespace,
})

const mapDispatchToProps = {
  getSource: getSourceAsync,
  getSources: getSourcesAsync,
  setTimeRange,
  setNamespace,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
