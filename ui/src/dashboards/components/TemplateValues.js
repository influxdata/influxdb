import React, {PropTypes} from 'react'

const TemplateValues = ({values, selectedType}) => {
  switch (selectedType) {
    case 'csv':
      return <div>{values.join(', ')}</div>
    case 'databases':
      return <div>SHOW DATABASES</div>
    case 'measurements':
      return <div>SHOW MEASUREMENTS on "db"</div>
    case 'fields':
      return <div>SHOW FIELD KEYS on "db" from "measurement"</div>
    case 'tagKeys':
      return <div>SHOW TAG KEYS ON "db" FROM "measurement"</div>
    case 'tagValues':
      return (
        <div>
          SHOW TAG VALUES ON "db_name" FROM "measurement_name" WITH KEY = "tag_key"
        </div>
      )
    default:
      return <div>{values.join(', ')}</div>
  }
}

const {arrayOf, string} = PropTypes

TemplateValues.propTypes = {
  selectedType: string.isRequired,
  values: arrayOf(string),
}

export default TemplateValues
