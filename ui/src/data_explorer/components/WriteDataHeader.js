import React, {PropTypes} from 'react'
import DatabaseDropdown from 'shared/components/DatabaseDropdown'

const WriteDataHeader = ({
  handleSelectDatabase,
  selectedDatabase,
  errorThrown,
  toggleWriteView,
  isManual,
  onClose,
}) => (
  <div className="write-data-form--header">
    <div className="page-header__left">
      <h1 className="page-header__title">Write Data To</h1>
      <DatabaseDropdown
        onSelectDatabase={handleSelectDatabase}
        database={selectedDatabase}
        onErrorThrown={errorThrown}
      />
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          onClick={() => toggleWriteView(false)}
          className={isManual ? '' : 'active'}
        >
          File Upload
        </li>
        <li
          onClick={() => toggleWriteView(true)}
          className={isManual ? 'active' : ''}
        >
          Manual Entry
        </li>
      </ul>
    </div>
    <div className="page-header__right">
      <span className="page-header__dismiss" onClick={onClose} />
    </div>
  </div>
)

const {func, string, bool} = PropTypes

WriteDataHeader.propTypes = {
  handleSelectDatabase: func.isRequired,
  selectedDatabase: string,
  toggleWriteView: func.isRequired,
  errorThrown: func.isRequired,
  onClose: func.isRequired,
  isManual: bool,
}

export default WriteDataHeader
