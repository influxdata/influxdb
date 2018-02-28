import AJAX from 'src/utils/ajax'

const msToRFC = ms => ms && new Date(parseInt(ms, 10)).toISOString()
const rfcToMS = rfc3339 => rfc3339 && JSON.stringify(Date.parse(rfc3339))
const annoToMillisecond = anno => ({
  ...anno,
  startTime: rfcToMS(anno.startTime),
  endTime: rfcToMS(anno.endTime),
})
const annoToRFC = anno => ({
  ...anno,
  startTime: msToRFC(anno.startTime),
  endTime: msToRFC(anno.endTime),
})

export const createAnnotation = async (url, annotation) => {
  const data = annoToRFC(annotation)
  const response = await AJAX({method: 'POST', url, data})
  return annoToMillisecond(response.data)
}

export const getAnnotations = async (url, since) => {
  const {data} = await AJAX({
    method: 'GET',
    url,
    params: {since: msToRFC(since)},
  })
  return data.annotations.map(annoToMillisecond)
}

export const deleteAnnotation = async annotation => {
  const url = annotation.links.self
  await AJAX({method: 'DELETE', url})
}

export const updateAnnotation = async annotation => {
  const url = annotation.links.self
  const data = annoToRFC(annotation)
  await AJAX({method: 'PATCH', url, data})
}
