import React, {PropTypes} from 'react'
import DatabaseDropdown from 'shared/components/DatabaseDropdown'
import MeasurementDropdown from 'src/dashboards/components/MeasurementDropdown'
import TagKeyDropdown from 'src/dashboards/components/TagKeyDropdown'

const TemplateQueryBuilder = ({
  selectedType,
  selectedDatabase,
  selectedMeasurement,
  selectedTagKey,
  onSelectDatabase,
  onSelectMeasurement,
  onSelectTagKey,
  onStartEdit,
  onErrorThrown,
}) => {
  switch (selectedType) {
    case 'csv':
      return null
    case 'databases':
      return <div className="tvm-query-builder--text">SHOW DATABASES</div>
    case 'measurements':
      return (
        <div className="tvm-query-builder">
          <span className="tvm-query-builder--text">SHOW MEASUREMENTS ON</span>
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
            onErrorThrown={onErrorThrown}
          />
        </div>
      )
    case 'fieldKeys':
    case 'tagKeys':
      return (
        <div className="tvm-query-builder">
          <span className="tvm-query-builder--text">
            SHOW {selectedType === 'fieldKeys' ? 'FIELD' : 'TAG'} KEYS ON
          </span>
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
            onErrorThrown={onErrorThrown}
          />
          <span className="tvm-query-builder--text">FROM</span>
          {selectedDatabase
            ? <MeasurementDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                onSelectMeasurement={onSelectMeasurement}
                onStartEdit={onStartEdit}
                onErrorThrown={onErrorThrown}
              />
            : <div>No database selected</div>}
        </div>
      )
    case 'tagValues':
      return (
        <div className="tvm-query-builder">
          <span className="tvm-query-builder--text">SHOW TAG VALUES ON</span>
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
            onErrorThrown={onErrorThrown}
          />
          <span className="tvm-query-builder--text">FROM</span>
          {selectedDatabase
            ? <MeasurementDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                onSelectMeasurement={onSelectMeasurement}
                onStartEdit={onStartEdit}
                onErrorThrown={onErrorThrown}
              />
            : 'Pick a DB'}
          <span className="tvm-query-builder--text">WITH KEY =</span>
          {selectedMeasurement
            ? <TagKeyDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                tagKey={selectedTagKey}
                onSelectTagKey={onSelectTagKey}
                onStartEdit={onStartEdit}
                onErrorThrown={onErrorThrown}
              />
            : 'Pick a Tag Key'}
        </div>
      )
    default:
      return (
        <div>
          <span className="tvm-query-builder--text">n/a</span>
        </div>
      )
  }
}

const {func, string} = PropTypes

TemplateQueryBuilder.propTypes = {
  selectedType: string.isRequired,
  onSelectDatabase: func.isRequired,
  onSelectMeasurement: func.isRequired,
  onSelectTagKey: func.isRequired,
  onStartEdit: func.isRequired,
  selectedMeasurement: string,
  selectedDatabase: string,
  selectedTagKey: string,
  onErrorThrown: func.isRequired,
}

export default TemplateQueryBuilder
