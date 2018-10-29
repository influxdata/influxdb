// download.js v4.2, by dandavis; 2008-2016. [CCBY2] see http://danml.com/download.html for tests/usage
// v1 landed a FF+Chrome compat way of downloading strings to local un-named files, upgraded to use a hidden frame and optional mime
// v2 added named files via a[download], msSaveBlob, IE (10+) support, and window.URL support for larger+faster saves than dataURLs
// v3 added dataURL and Blob Input, bind-toggle arity, and legacy dataURL fallback was improved with force-download mime and base64 support. 3.1 improved safari handling.
// v4 adds AMD/UMD, commonJS, and plain browser support
// v4.1 adds url download capability via solo URL argument (same domain/CORS only)
// v4.2 adds semantic variable names, long (over 2MB) dataURL support, and hidden by default temp anchors
// https://github.com/rndme/download
/* tslint:disable */

const dataUrlToBlob = (myBlob, strUrl) => {
  const parts = strUrl.split(/[:;,]/),
    type = parts[1],
    decoder = parts[2] === 'base64' ? atob : decodeURIComponent,
    binData = decoder(parts.pop()),
    mx = binData.length,
    uiArr = new Uint8Array(mx)

  for (let i = 0; i < mx; ++i) {
    uiArr[i] = binData.charCodeAt(i)
  }

  return new myBlob([uiArr], {type})
}

const download = (data, strFileName, strMimeType) => {
  const _window = window // this script is only for browsers anyway...
  const defaultMime = 'application/octet-stream' // this default mime also triggers iframe downloads
  let mimeType = strMimeType || defaultMime
  let payload = data
  let url = !strFileName && !strMimeType && payload
  const anchor = document.createElement('a')
  const toString = a => `${a}`
  let myBlob: any = _window.Blob || toString
  let fileName = strFileName || 'download'
  let reader
  myBlob = myBlob.call ? myBlob.bind(_window) : Blob

  if (url && url.length < 2048) {
    // if no filename and no mime, assume a url was passed as the only argument
    fileName = url
      .split('/')
      .pop()
      .split('?')[0]
    anchor.href = url // assign href prop to temp anchor
    if (anchor.href.indexOf(url) !== -1) {
      // if the browser determines that it's a potentially valid url path:
      const ajax = new XMLHttpRequest()
      ajax.open('GET', url, true)
      ajax.responseType = 'blob'
      ajax.onload = function(e: any) {
        download(e.target.response, fileName, defaultMime)
      }
      setTimeout(function() {
        ajax.send()
      }, 0) // allows setting custom ajax headers using the return:
      return ajax
    } // end if valid url?
  } // end if url?

  const saver = (saverUrl, winMode) => {
    if ('download' in anchor) {
      // html5 A[download]
      anchor.href = saverUrl
      anchor.setAttribute('download', fileName)
      anchor.className = 'download-js-link'
      anchor.innerHTML = 'downloading...'
      anchor.style.display = 'none'
      document.body.appendChild(anchor)
      setTimeout(function() {
        anchor.click()
        document.body.removeChild(anchor)
        if (winMode === true) {
          setTimeout(function() {
            _window.URL.revokeObjectURL(anchor.href)
          }, 250)
        }
      }, 66)
      return true
    }

    // do iframe dataURL download (old ch+FF):
    const f = document.createElement('iframe')
    document.body.appendChild(f)

    if (!winMode) {
      // force a mime that will download:
      url = `data:${url.replace(/^data:([\w\/\-\+]+)/, defaultMime)}`
    }
    f.src = url
    setTimeout(function() {
      document.body.removeChild(f)
    }, 333)
  } // end saver

  // go ahead and download dataURLs right away
  if (/^data\:[\w+\-]+\/[\w+\-]+[,;]/.test(payload)) {
    if (payload.length > 1024 * 1024 * 1.999 && myBlob !== toString) {
      payload = dataUrlToBlob(myBlob, payload)
      mimeType = payload.type || defaultMime
    } else {
      return navigator.msSaveBlob // IE10 can't do a[download], only Blobs:
        ? navigator.msSaveBlob(dataUrlToBlob(myBlob, payload), fileName)
        : saver(payload, false) // everyone else can save dataURLs un-processed
    }
  } // end if dataURL passed?

  const blob =
    payload instanceof myBlob
      ? payload
      : new myBlob([payload], {type: mimeType})

  if (navigator.msSaveBlob) {
    // IE10+ : (has Blob, but not a[download] or URL)
    return navigator.msSaveBlob(blob, fileName)
  }

  if (_window.URL) {
    // simple fast and modern way using Blob and URL:
    saver(_window.URL.createObjectURL(blob), true)
  } else {
    // handle non-Blob()+non-URL browsers:
    if (typeof blob === 'string' || blob.constructor === toString) {
      try {
        return saver(`data:${mimeType};base64,${_window.btoa(blob)}`, false)
      } catch (y) {
        return saver(`data:${mimeType},${encodeURIComponent(blob)}`, false)
      }
    }

    // Blob but not URL support:
    reader = new FileReader()
    reader.onload = function() {
      saver(this.result, false)
    }
    reader.readAsDataURL(blob)
  }
  return true
} /* end download() */

export default download
