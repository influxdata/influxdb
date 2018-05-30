import React, {SFC} from 'react'

const SearchSpinner: SFC = () => {
  return (
    <div className="search-spinner">
      <div className="spinner">
        <div className="bounce1" />
        <div className="bounce2" />
        <div className="bounce3" />
      </div>
    </div>
  )
}

export default SearchSpinner
