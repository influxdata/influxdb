// Libraries
import React, {Component, ChangeEvent} from 'react'
import {debounce} from 'lodash'

// Components
import {Input} from '@influxdata/clockface'

// Types
import {IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onSearch: (searchTerm: string) => void
  placeholderText: string
  searchTerm: string
  testID: string
}

interface State {
  searchTerm: string
}

@ErrorHandling
class SearchWidget extends Component<Props, State> {
  public static defaultProps = {
    widthPixels: 440,
    placeholderText: 'Search...',
    searchTerm: '',
    testID: 'search-widget',
  }

  public componentDidUpdate(prevProps: Props) {
    if (this.props.searchTerm !== prevProps.searchTerm) {
      this.setState({searchTerm: this.props.searchTerm})
    }
  }

  constructor(props: Props) {
    super(props)
    this.state = {
      searchTerm: this.props.searchTerm,
    }
  }

  public UNSAFE_componentWillMount() {
    this.handleSearch = debounce(this.handleSearch, 50)
  }

  public render() {
    const {placeholderText, testID} = this.props
    const {searchTerm} = this.state

    return (
      <Input
        icon={IconFont.Search}
        placeholder={placeholderText}
        value={searchTerm}
        onChange={this.handleChange}
        onBlur={this.handleBlur}
        testID={testID}
        className="search-widget-input"
      />
    )
  }

  private handleSearch = (): void => {
    this.props.onSearch(this.state.searchTerm)
  }

  private handleBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value}, this.handleSearch)
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value}, this.handleSearch)
  }
}

export default SearchWidget
