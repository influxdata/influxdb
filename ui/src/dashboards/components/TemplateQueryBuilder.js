import React, {PropTypes} from 'react'
import DatabaseDropdown from 'src/dashboards/components/DatabaseDropdown'
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
}) => {
  switch (selectedType) {
    case 'csv':
      return <div>n/a</div>
    case 'databases':
      return <div>SHOW DATABASES</div>
    case 'measurements':
      return (
        <div>
          <span>SHOW MEASUREMENTS ON</span>
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
          />
        </div>
      )
    case 'fieldKeys':
    case 'tagKeys':
      return (
        <div>
          SHOW {selectedType === 'fieldKeys' ? 'FIELD' : 'TAG'} KEYS ON
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
          />
          FROM
          {selectedDatabase
            ? <MeasurementDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                onSelectMeasurement={onSelectMeasurement}
                onStartEdit={onStartEdit}
              />
            : <div>No database selected</div>}
        </div>
      )
    case 'tagValues':
      return (
        <div>
          SHOW TAG VALUES ON
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
            onStartEdit={onStartEdit}
          />
          FROM
          {selectedDatabase
            ? <MeasurementDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                onSelectMeasurement={onSelectMeasurement}
                onStartEdit={onStartEdit}
              />
            : 'Pick a DB'}
          WITH KEY =
          {selectedMeasurement
            ? <TagKeyDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                tagKey={selectedTagKey}
                onSelectTagKey={onSelectTagKey}
                onStartEdit={onStartEdit}
              />
            : 'Pick a Tag Key'}
        </div>
      )
    default:
      return <div>n/a</div>
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
}

export default TemplateQueryBuilder
