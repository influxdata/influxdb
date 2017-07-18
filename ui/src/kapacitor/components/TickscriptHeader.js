import React, {PropTypes} from 'react'
import SourceIndicator from 'shared/components/SourceIndicator'
import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'shared/components/MultiSelectDBDropdown'
import TickscrtiptID from 'src/kapacitor/components/TickscriptID'
const addName = list => list.map(l => ({...l, name: `${l.db}.${l.rp}`}))

const TickscriptHeader = ({
  task: {id, type, dbrps},
  source: {name},
  onSave,
  isEditing,
  onStopEdit,
  onStartEdit,
  onChangeType,
  onSelectDbrps,
}) => (
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <TickscrtiptID
          id={id}
          isEditing={isEditing}
          onStopEdit={onStopEdit}
          onStartEdit={onStartEdit}
        />
      </div>
      <div className="page-header__right">
        <SourceIndicator sourceName={name} />
        <TickscriptType type={type} onChangeType={onChangeType} />
        <MultiSelectDBDropdown
          selectedItems={addName(dbrps)}
          onApply={onSelectDbrps}
        />
        <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>
      </div>
    </div>
  </div>
)

const {arrayOf, bool, func, shape, string} = PropTypes

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
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  onStopEdit: func.isRequired,
}

export default TickscriptHeader
