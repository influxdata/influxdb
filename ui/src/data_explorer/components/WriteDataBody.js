import React, {PropTypes} from 'react'
import WriteDataFooter from 'src/data_explorer/components/WriteDataFooter'

const WriteDataBody = ({
  handleKeyUp,
  handleCancelFile,
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
      : <div
          className={
            uploadContent
              ? 'write-data-form--file'
              : 'write-data-form--file write-data-form--file_active'
          }
          onClick={handleFileOpen}
        >
          {uploadContent
            ? <h3 className="write-data-form--filepath_selected">
                {fileName}
              </h3>
            : <h3 className="write-data-form--filepath_empty">
                Drop a file here or click to upload
              </h3>}
          <div
            className={
              uploadContent
                ? 'write-data-form--graphic write-data-form--graphic_success'
                : 'write-data-form--graphic'
            }
          />
          <input
            type="file"
            onChange={handleFile(false)}
            className="write-data-form--upload"
            ref={fileInput}
            accept="text/*, application/gzip"
          />
          {uploadContent &&
            <span className="write-data-form--file-submit">
              <button className="btn btn-md btn-success" onClick={handleSubmit}>
                Write this File
              </button>
              <button
                className="btn btn-md btn-default"
                onClick={handleCancelFile}
              >
                Cancel
              </button>
            </span>}
        </div>}
    {isManual &&
      <WriteDataFooter
        isUploading={isUploading}
        isManual={isManual}
        inputContent={inputContent}
        handleSubmit={handleSubmit}
        uploadContent={uploadContent}
      />}
  </div>

const {func, string, bool} = PropTypes

WriteDataBody.propTypes = {
  handleKeyUp: func.isRequired,
  handleEdit: func.isRequired,
  handleCancelFile: func.isRequired,
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
