import React, {PropTypes, Component} from 'react'
import _ from 'lodash'

class SearchBar extends Component {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  componentWillMount() {
    this.handleSearch = _.debounce(this.handleSearch, 50)
  }

  handleSearch = () => {
    this.props.onSearch(this.state.searchTerm)
  }

  handleChange = () => {
    this.setState({searchTerm: this.refs.searchInput.value}, this.handleSearch)
  }

  render() {
    const {placeholder} = this.props
    return (
      <div className="users__search-widget input-group">
        <input
          type="text"
          className="form-control"
          placeholder={placeholder}
          ref="searchInput"
          onChange={this.handleChange}
        />
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true" />
        </div>
      </div>
    )
  }
}

const {func, string} = PropTypes

SearchBar.propTypes = {
  onSearch: func.isRequired,
  placeholder: string.isRequired,
}

export default SearchBar
