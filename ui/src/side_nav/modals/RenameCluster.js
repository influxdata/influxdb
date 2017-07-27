import React, {PropTypes} from 'react'

const {func} = PropTypes
const RenameCluster = React.createClass({
  propTypes: {
    onRenameCluster: func.isRequired,
  },

  handleRenameCluster(e) {
    e.preventDefault()
    this.props.onRenameCluster(this._displayName.value)
    this._displayName.value = ''

    $('.modal').hide() // eslint-disable-line no-undef
    $('.modal-backdrop').hide() // eslint-disable-line no-undef
  },

  render() {
    return (
      <div
        className="modal fade"
        tabIndex="-1"
        role="dialog"
        id="rename-cluster-modal"
      >
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button
                type="button"
                className="close"
                data-dismiss="modal"
                aria-label="Close"
              >
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Rename Cluster</h4>
            </div>
            <form onSubmit={this.handleRenameCluster}>
              <div className="modal-body">
                <div className="row">
                  <div className="col-md-8 col-md-offset-2 text-center">
                    <p>
                      A cluster can have an alias that replaces its ID in the
                      interface.
                      <br />
                      This does not affect the cluster ID.
                    </p>
                    <div className="form-group">
                      <label htmlFor="cluster-alias" className="sr-only">
                        Cluster Alias
                      </label>
                      <input
                        ref={ref => (this._displayName = ref)}
                        required={true}
                        type="text"
                        className="input-lg form-control"
                        maxLength="22"
                        id="cluster-alias"
                        placeholder="Name this cluster..."
                      />
                    </div>
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button
                  type="button"
                  className="btn btn-default"
                  data-dismiss="modal"
                >
                  Cancel
                </button>
                <button
                  disabled={!this._displayName}
                  type="submit"
                  className="btn btn-primary js-rename-cluster"
                >
                  Rename
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    )
  },
})

export default RenameCluster
