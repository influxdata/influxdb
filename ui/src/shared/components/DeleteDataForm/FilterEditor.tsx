// Libraries
import React, {FunctionComponent} from 'react'
import {Button, IconFont, ButtonShape, Panel} from '@influxdata/clockface'

// Components
import FilterRow from 'src/shared/components/DeleteDataForm/FilterRow'

// Types
import {Filter} from 'src/types'

interface Props {
  bucket: string
  filters: Filter[]
  keys: string[]
  onDeleteFilter: (index: number) => any
  onSetFilter: (filter: Filter, index: number) => any
  shouldValidate: boolean
  values: (string | number)[]
}

const FilterEditor: FunctionComponent<Props> = ({
  bucket,
  filters,
  keys,
  onDeleteFilter,
  onSetFilter,
  shouldValidate,
  values,
}) => {
  return (
    <div className="delete-data-filters">
      <Button
        text="Add Filter"
        testID="add-filter-btn"
        icon={IconFont.Plus}
        shape={ButtonShape.StretchToFit}
        className="delete-data-filters--new-filter"
        onClick={() =>
          onSetFilter({key: '', equality: '=', value: ''}, filters.length)
        }
      />
      {filters.length > 0 ? (
        <div className="delete-data-filters--filters">
          {filters.map((filter, i) => (
            <FilterRow
              bucket={bucket}
              key={i}
              keys={keys}
              filter={filter}
              onChange={filter => onSetFilter(filter, i)}
              onDelete={() => onDeleteFilter(i)}
              shouldValidate={shouldValidate}
              values={values}
            />
          ))}
        </div>
      ) : (
        <Panel className="delete-data-filters--no-filters">
          <Panel.Body>
            <p>
              If no filters are specified, all data points in the selected time
              range will be marked for deletion.
            </p>
          </Panel.Body>
        </Panel>
      )}
    </div>
  )
}

export default FilterEditor
