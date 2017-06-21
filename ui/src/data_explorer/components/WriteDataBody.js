import React, {PropTypes} from 'react'
import WriteDataFooter from 'src/data_explorer/components/WriteDataFooter'

const WriteDataBody = ({
  handleKeyUp,
  handleFile,
  handleEdit,
  handleSubmit,
  inputContent,
  uploadContent,
  isManual,
}) => {
  let inputRef

  return (
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
          />
        : <div className="write-data-form--file">
            <input
              type="file"
              onChange={handleFile}
              className="write-data-form--upload"
              ref={r => inputRef = r}
              accept="text/*, application/gzip"
            />
            <button
              className="btn btn-sm btn-primary"
              onClick={() => inputRef.click()}
            >
              {uploadContent ? 'Upload a Different File' : 'Upload a File'}
            </button>
            {uploadContent
              ? <span className="write-data-form--filepath_selected">
                  <span className="icon checkmark" />1 file selected
                </span>
              : <span className="write-data-form--filepath_empty">
                  No file selected
                </span>}
          </div>}
      <WriteDataFooter
        isManual={isManual}
        inputContent={inputContent}
        handleSubmit={handleSubmit}
        uploadContent={uploadContent}
      />
    </div>
  )
}

const {func, string, bool} = PropTypes

WriteDataBody.propTypes = {
  handleKeyUp: func.isRequired,
  handleEdit: func.isRequired,
  handleFile: func.isRequired,
  handleSubmit: func.isRequired,
  inputContent: string,
  uploadContent: string,
  isManual: bool,
}

export default WriteDataBody
