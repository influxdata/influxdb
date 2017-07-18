import React, {PropTypes} from 'react'
import SourceIndicator from 'shared/components/SourceIndicator'
import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'shared/components/MultiSelectDBDropdown'

const addName = list => list.map(l => ({...l, name: `${l.db}.${l.rp}`}))

const TickscriptHeader = ({
  source,
  onChangeType,
  onSave,
  task,
  onSelectDbrps,
}) => (
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title kapacitor-theme">
          TICKscript Editor
        </h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator sourceName={source.name} />
        <TickscriptType type={task.type} onChangeType={onChangeType} />
        <MultiSelectDBDropdown
          selectedItems={addName(task.dbrps)}
          onApply={onSelectDbrps}
        />
        <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>
      </div>
    </div>
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

TickscriptHeader.propTypes = {
  onSave: func,
  source: shape(),
  onSelectDbrps: func.isRequired,
  task: shape({
    dbrps: arrayOf(
      shape({
        db: string,
        rp: string,
      })
    ),
  }),
  onChangeType: func.isRequired,
}

export default TickscriptHeader
