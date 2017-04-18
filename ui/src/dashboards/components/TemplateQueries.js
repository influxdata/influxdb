import React, {PropTypes} from 'react'
import DatabaseDropdown from 'src/dashboards/components/DatabaseDropdown'
import MeasurementDropdown from 'src/dashboards/components/MeasurementDropdown'

const TemplateQueries = ({
  selectedType,
  selectedDatabase,
  selectedMeasurement,
  onSelectDatabase,
  onSelectMeasurement,
}) => {
  switch (selectedType) {
    case 'csv':
      return <div>n/a</div>
    case 'databases':
      return <div>SHOW DATABASES</div>
    case 'measurements':
      return (
        <div>
          <span>SHOW MEASUREMENTS on</span>
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
          />
        </div>
      )
    case 'fields':
      return (
        <div>
          SHOW FIELD KEYS on
          <DatabaseDropdown
            onSelectDatabase={onSelectDatabase}
            database={selectedDatabase}
          />
          from
          {selectedDatabase
            ? <MeasurementDropdown
                database={selectedDatabase}
                measurement={selectedMeasurement}
                onSelectMeasurement={onSelectMeasurement}
              />
            : 'Pick a DB'}
        </div>
      )
    case 'tagKeys':
      return <div>SHOW TAG KEYS ON "db" FROM "measurement"</div>
    case 'tagValues':
      return (
        <div>
          SHOW TAG VALUES ON "db_name" FROM "measurement_name" WITH KEY = "tag_key"
        </div>
      )
    default:
      return <div>n/a</div>
  }
}

const {func, string} = PropTypes

TemplateQueries.propTypes = {
  selectedType: string.isRequired,
  onSelectDatabase: func.isRequired,
  onSelectMeasurement: func.isRequired,
  selectedMeasurement: string,
  selectedDatabase: string,
}

export default TemplateQueries
