import React, {PropTypes} from 'react'
import SourceIndicator from 'shared/components/SourceIndicator'
import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'shared/components/MultiSelectDBDropdown'
import TickscriptID, {
  TickscriptStaticID,
} from 'src/kapacitor/components/TickscriptID'
import LogsToggle from 'src/kapacitor/components/LogsToggle'

const addName = list => list.map(l => ({...l, name: `${l.db}.${l.rp}`}))

const TickscriptHeader = ({
  task: {id, type, dbrps},
  task,
  onSave,
  onChangeType,
  onChangeID,
  onSelectDbrps,
  isNewTickscript,
  areLogsVisible,
  onToggleLogsVisbility,
}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        {isNewTickscript
          ? <TickscriptID onChangeID={onChangeID} id={id} />
          : <TickscriptStaticID id={task.name} />}
      </div>
      <div className="page-header__right">
        <SourceIndicator />
        <LogsToggle
          areLogsVisible={areLogsVisible}
          onToggleLogsVisbility={onToggleLogsVisbility}
        />
        <TickscriptType type={type} onChangeType={onChangeType} />
        <MultiSelectDBDropdown
          selectedItems={addName(dbrps)}
          onApply={onSelectDbrps}
        />
        <button
          className="btn btn-success btn-sm"
          title={id ? '' : 'ID your TICKscript to save'}
          onClick={onSave}
          disabled={!id}
        >
          Save Rule
        </button>
      </div>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TickscriptHeader.propTypes = {
  onSave: func,
  onSelectDbrps: func.isRequired,
  areLogsVisible: bool,
  onToggleLogsVisbility: func.isRequired,
  task: shape({
    dbrps: arrayOf(
      shape({
        db: string,
        rp: string,
      })
    ),
  }),
  onChangeType: func.isRequired,
  onChangeID: func.isRequired,
  isNewTickscript: bool.isRequired,
}

export default TickscriptHeader
