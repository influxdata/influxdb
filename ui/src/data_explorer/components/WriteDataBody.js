import React, {PropTypes} from 'react'
import WriteDataFooter from 'src/data_explorer/components/WriteDataFooter'

const WriteDataBody = ({
  handleKeyUp,
  handleFile,
  handleEdit,
  handleSubmit,
  inputContent,
  uploadContent,
  fileName,
  isManual,
  fileInput,
  handleFileOpen,
  isUploading,
}) =>
  <div className="write-data-form--body">
    {isManual
      ? <textarea
          className="form-control write-data-form--input"
          autoComplete="off"
          spellCheck="false"
          placeholder="<measurement>,<tag_key>=<tag_value> <field_key>=<field_value>"
          onKeyUp={handleKeyUp}
          onChange={handleEdit}
          autoFocus={true}
          data-test="manual-entry-field"
        />
      : <div className="write-data-form--file">
          <h3>Drag & Drop a File</h3>
          <div className="write-data-form--drag-graphic" />
          <p>OR</p>
          <input
            type="file"
            onChange={e => handleFile(e, false)}
            className="write-data-form--upload"
            ref={fileInput}
            accept="text/*, application/gzip"
          />
          <button className="btn btn-md btn-primary" onClick={handleFileOpen}>
            {uploadContent
              ? 'Choose Another File to Upload'
              : 'Choose a File to Upload'}
          </button>
          {uploadContent
            ? <span className="write-data-form--filepath_selected">
                <span className="icon checkmark" />
                {fileName}
              </span>
            : <span className="write-data-form--filepath_empty">
                No file selected
              </span>}
        </div>}
    <WriteDataFooter
      isUploading={isUploading}
      isManual={isManual}
      inputContent={inputContent}
      handleSubmit={handleSubmit}
      uploadContent={uploadContent}
    />
  </div>

const {func, string, bool} = PropTypes

WriteDataBody.propTypes = {
  handleKeyUp: func.isRequired,
  handleEdit: func.isRequired,
  handleFile: func.isRequired,
  handleSubmit: func.isRequired,
  inputContent: string,
  uploadContent: string,
  fileName: string,
  isManual: bool,
  fileInput: func.isRequired,
  handleFileOpen: func.isRequired,
  isUploading: bool.isRequired,
}

export default WriteDataBody
