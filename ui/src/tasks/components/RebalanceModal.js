import React, {PropTypes} from 'react';

const {func} = PropTypes;
const RebalanceModal = React.createClass({
  propTypes: {
    onConfirmRebalance: func.isRequired,
  },

  render() {
    return (
      <div className="modal fade" id="rebalanceModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title" id="myModalLabel">This is a potentially heavy operation. <br/> Are you sure you want to rebalance?</h4>
            </div>
            <div className="modal-footer">
              <button type="button" className="btn btn-default" data-dismiss="modal">No</button>
              <button type="button" className="btn btn-success" data-dismiss="modal" onClick={this.props.onConfirmRebalance}>Yes, run it!</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default RebalanceModal;
