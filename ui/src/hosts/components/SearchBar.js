import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
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
    const {placeholder, width} = this.props
    return (
      <div className="search-widget" style={{width: `${width}px`}}>
        <input
          type="text"
          className="form-control input-sm"
          placeholder={placeholder}
          ref="searchInput"
          onChange={this.handleChange}
        />
        <span className="icon search" />
      </div>
    )
  }
}

const {func, number, string} = PropTypes

SearchBar.defaultProps = {
  width: 260,
}

SearchBar.propTypes = {
  width: number,
  onSearch: func.isRequired,
  placeholder: string.isRequired,
}

export default SearchBar
